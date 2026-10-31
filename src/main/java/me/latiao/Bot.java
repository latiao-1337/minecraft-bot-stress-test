package me.latiao;

import net.kyori.adventure.key.Key;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.cloudburstmc.nbt.NbtMap;
import org.geysermc.mcprotocollib.network.ProxyInfo;
import org.geysermc.mcprotocollib.network.Session;
import org.geysermc.mcprotocollib.network.event.session.DisconnectedEvent;
import org.geysermc.mcprotocollib.network.event.session.SessionAdapter;
import org.geysermc.mcprotocollib.network.factory.ClientNetworkSessionFactory;
import org.geysermc.mcprotocollib.network.packet.Packet;
import org.geysermc.mcprotocollib.network.session.ClientNetworkSession;
import org.geysermc.mcprotocollib.protocol.MinecraftProtocol;
import org.geysermc.mcprotocollib.protocol.packet.common.serverbound.ServerboundCustomClickActionPacket;
import org.geysermc.mcprotocollib.protocol.packet.configuration.clientbound.ClientboundShowDialogConfigurationPacket;
import org.geysermc.mcprotocollib.protocol.packet.ingame.clientbound.*;
import org.geysermc.mcprotocollib.protocol.packet.ingame.serverbound.ServerboundChatCommandPacket;
import org.geysermc.mcprotocollib.protocol.packet.ingame.serverbound.ServerboundChatPacket;
import org.geysermc.mcprotocollib.protocol.packet.status.clientbound.ClientboundStatusResponsePacket;
import org.geysermc.mcprotocollib.protocol.packet.status.serverbound.ServerboundStatusRequestPacket;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Bot {

    private static final Path CONFIG_FILE = Paths.get("config.properties");
    private static Config CONFIG;
    private static final Random RANDOM = new Random();
    private static final PlainTextComponentSerializer PLAIN = PlainTextComponentSerializer.plainText();
    private static final Pattern CAPTCHA_PATTERN = Pattern.compile("/captcha\\s+(\\S+)");

    private static final Object JOIN_LOCK = new Object();
    private static final ArrayDeque<Long> RECENT_JOINS = new ArrayDeque<>();
    private static long lastJoinAt = 0L;

    public static void main(String[] args) throws Exception {
        CONFIG = Config.load(CONFIG_FILE);
        if (CONFIG == null) {
            return;
        }

        List<String> proxies = loadProxies();
        if (proxies.isEmpty()) {
            System.err.println("Proxy list is empty, check " + CONFIG.proxyFile.toAbsolutePath());
            return;
        }

        int total = proxies.size();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < total; i++) {
            final int index = i + 1;
            final String proxyStr = proxies.get(i);
            Thread t = new Thread(() -> runBot(index, total, proxyStr), "bot-" + index);
            t.start();
            threads.add(t);
        }

        System.out.println("Started " + threads.size() + " bot threads");
        printRateLimitConfig();

        for (Thread t : threads) {
            t.join();
        }
    }

    private static void printRateLimitConfig() {
        String perMinute = CONFIG.maxJoinsPerMinute > 0
                ? CONFIG.maxJoinsPerMinute + " per minute" : "unlimited";
        String interval = CONFIG.joinIntervalSeconds > 0
                ? CONFIG.joinIntervalSeconds + "s per join" : "unlimited";
        System.out.println("Rate limit: perMinute=" + perMinute + ", interval=" + interval);
    }

    private static void waitForJoinSlot() throws InterruptedException {
        final long minuteMs = 60_000L;
        final long intervalMs = CONFIG.joinIntervalSeconds > 0 ? CONFIG.joinIntervalSeconds * 1000L : 0L;

        if (CONFIG.maxJoinsPerMinute <= 0 && intervalMs <= 0) {
            return;
        }

        while (true) {
            long waitMs;
            synchronized (JOIN_LOCK) {
                long now = System.currentTimeMillis();

                if (CONFIG.maxJoinsPerMinute > 0) {
                    while (!RECENT_JOINS.isEmpty() && now - RECENT_JOINS.peekFirst() >= minuteMs) {
                        RECENT_JOINS.pollFirst();
                    }
                }

                long waitByInterval = 0L;
                if (intervalMs > 0 && lastJoinAt > 0) {
                    long target = lastJoinAt + intervalMs;
                    if (target > now) {
                        waitByInterval = target - now;
                    }
                }

                long waitByRate = 0L;
                if (CONFIG.maxJoinsPerMinute > 0 && RECENT_JOINS.size() >= CONFIG.maxJoinsPerMinute) {
                    long target = RECENT_JOINS.peekFirst() + minuteMs;
                    if (target > now) {
                        waitByRate = target - now;
                    }
                }

                waitMs = Math.max(waitByInterval, waitByRate);

                if (waitMs <= 0) {
                    long stamp = System.currentTimeMillis();
                    if (CONFIG.maxJoinsPerMinute > 0) {
                        RECENT_JOINS.addLast(stamp);
                    }
                    lastJoinAt = stamp;
                    return;
                }
            }

            Thread.sleep(Math.min(waitMs, 500L));
        }
    }

    private static void runBot(int index, int total, String proxyStr) {
        for (int attempt = 1; attempt <= CONFIG.maxAttempts; attempt++) {
            if (CONFIG.queryServerStatus) {
                queryServerStatus(proxyStr);
                try {
                    Thread.sleep(CONFIG.statusQueryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            String username = generateRandomName();
            AtomicBoolean loggedIn = new AtomicBoolean(false);
            AtomicBoolean disconnected = new AtomicBoolean(false);

            try {
                waitForJoinSlot();

                ClientNetworkSession session = createBot(username, proxyStr, loggedIn, disconnected);
                System.out.println("[" + index + "/" + total + "] attempt #" + attempt
                        + " starting bot: " + username + " (proxy: " + proxyStr + ")");

                long deadline = System.currentTimeMillis() + CONFIG.loginTimeoutMs;
                while (!loggedIn.get() && !disconnected.get()
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(100);
                }

                if (!loggedIn.get()) {
                    System.out.println("[" + index + "/" + total + "] attempt #" + attempt
                            + " failed (not logged in), retrying");
                    safeDisconnect(session);
                    Thread.sleep(CONFIG.retryDelayMs);
                    continue;
                }

                Thread.sleep(1000);

                int msgIndex = 0;
                while (!disconnected.get() && session.isConnected()) {
                    String msg = CONFIG.chatMessages.get(msgIndex);
                    sendAny(session, msg);
                    msgIndex = (msgIndex + 1) % CONFIG.chatMessages.size();

                    long slept = 0;
                    while (slept < CONFIG.chatIntervalMs && !disconnected.get() && session.isConnected()) {
                        long step = Math.min(100, CONFIG.chatIntervalMs - slept);
                        Thread.sleep(step);
                        slept += step;
                    }
                }

                System.out.println("[" + index + "/" + total + "] bot " + username
                        + " disconnected, attempting #" + (attempt + 1));
                safeDisconnect(session);
                Thread.sleep(CONFIG.retryDelayMs);

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                System.err.println("[" + index + "/" + total + "] attempt #" + attempt
                        + " error: " + e.getMessage());
                try {
                    Thread.sleep(CONFIG.retryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        System.out.println("[" + index + "/" + total + "] proxy " + proxyStr
                + " failed after " + CONFIG.maxAttempts + " attempts, thread exiting");
    }

    private static void queryServerStatus(String proxyStr) {
        AtomicReference<ClientNetworkSession> sessionRef = new AtomicReference<>();
        AtomicBoolean gotResponse = new AtomicBoolean(false);

        try {
            MinecraftProtocol protocol = new MinecraftProtocol();

            String[] parts = proxyStr.split(":");
            ProxyInfo proxyInfo = new ProxyInfo(
                    ProxyInfo.Type.SOCKS5,
                    parts[0],
                    Integer.parseInt(parts[1])
            );

            ClientNetworkSession statusSession = ClientNetworkSessionFactory.factory()
                    .setAddress(CONFIG.host, CONFIG.port)
                    .setProtocol(protocol)
                    .setProxy(proxyInfo)
                    .create();

            sessionRef.set(statusSession);

            statusSession.addListener(new SessionAdapter() {
                @Override
                public void packetReceived(Session session, Packet packet) {
                    if (packet instanceof ClientboundStatusResponsePacket resp) {
                        gotResponse.set(true);
                        var info = resp.parseInfo();
                        String motd = info.getDescription() == null
                                ? "" : PLAIN.serialize(info.getDescription());
                        var ver = info.getVersionInfo();
                        var players = info.getPlayerInfo();

                        System.out.println("[status][" + proxyStr + "]");
                        System.out.println("  MOTD: " + motd);
                        if (ver != null) {
                            System.out.println("  version: " + ver.getVersionName()
                                    + " (protocol " + ver.getProtocolVersion() + ")");
                        }
                        if (players != null) {
                            System.out.println("  players: " + players.getOnlinePlayers()
                                    + "/" + players.getMaxPlayers());
                        }
                    }
                }
            });

            statusSession.connect();

            Thread.sleep(300);
            statusSession.send(new ServerboundStatusRequestPacket());

            long deadline = System.currentTimeMillis() + CONFIG.statusQueryWaitMs;
            while (!gotResponse.get() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }

            if (!gotResponse.get()) {
                System.out.println("[status][" + proxyStr + "] no response");
            }

        } catch (Exception e) {
            System.err.println("[status][" + proxyStr + "] query failed: " + e.getMessage());
        } finally {
            ClientNetworkSession s = sessionRef.get();
            if (s != null) {
                try {
                    if (s.isConnected()) s.disconnect("status query done");
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static void safeDisconnect(ClientNetworkSession session) {
        try {
            if (session != null && session.isConnected()) {
                session.disconnect("retry");
            }
        } catch (Exception ignored) {
        }
    }

    private static void sendAny(ClientNetworkSession session, String text) {
        if (!session.isConnected()) return;
        if (text.startsWith("/")) {
            sendCommand(session, text.substring(1));
        } else {
            String noisy = CONFIG.addRandomNoise ? addNoise(text) : text;
            sendChat(session, noisy);
        }
    }

    private static String addNoise(String input) {
        StringBuilder sb = new StringBuilder(input.length() * 2);
        for (int i = 0; i < input.length(); i++) {
            sb.append(input.charAt(i));
            if (RANDOM.nextDouble() < CONFIG.noiseProbability && !CONFIG.noiseChars.isEmpty()) {
                sb.append(CONFIG.noiseChars.charAt(RANDOM.nextInt(CONFIG.noiseChars.length())));
            }
        }
        return sb.toString();
    }

    private static void sendChat(ClientNetworkSession session, String message) {
        if (!session.isConnected()) return;
        try {
            session.send(new ServerboundChatPacket(
                    message,
                    System.currentTimeMillis(),
                    0L,
                    null,
                    0,
                    new BitSet(),
                    0
            ));
        } catch (Exception e) {
            System.err.println("sendChat failed: " + e.getMessage());
        }
    }

    private static void sendCommand(ClientNetworkSession session, String command) {
        if (!session.isConnected()) return;
        try {
            session.send(new ServerboundChatCommandPacket(command));
        } catch (Exception e) {
            System.err.println("sendCommand failed: " + e.getMessage());
        }
    }

    private static void handleChatText(String text, ClientNetworkSession session, String username) {
        if (CONFIG.autoCaptcha && text != null) {
            Matcher m = CAPTCHA_PATTERN.matcher(text);
            if (m.find()) {
                String code = m.group(1);
                System.out.println("[" + username + "] captcha detected, sending /captcha " + code);
                sendCommand(session, "captcha " + code);
            }
        }
    }

    private static void handleDialog(ClientboundShowDialogGamePacket packet,
                                     ClientNetworkSession session,
                                     String username) {
        try {
            if (packet.getDialog().isId()) {
                System.out.println("[" + username + "] dialog reference (id="
                        + packet.getDialog().id() + "), content unavailable");
                return;
            }
            NbtMap dialog = packet.getDialog().custom();
            processDialogNbt(dialog, session, username);
        } catch (Exception e) {
            System.err.println("handleDialog(game) failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void handleDialog(ClientboundShowDialogConfigurationPacket packet,
                                     ClientNetworkSession session,
                                     String username) {
        try {
            NbtMap dialog = packet.getDialog();
            processDialogNbt(dialog, session, username);
        } catch (Exception e) {
            System.err.println("handleDialog(config) failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processDialogNbt(NbtMap dialog, ClientNetworkSession session, String username) {
        System.out.println("[" + username + "] dialog nbt: " + dialog);

        if (!CONFIG.autoAuth) return;

        String title = extractTitle(dialog);
        String actionId = extractActionId(dialog);
        System.out.println("[" + username + "] dialog title: " + title
                + ", action id: " + actionId);

        boolean isRegister = false;
        boolean isLogin = false;
        if (actionId != null) {
            String id = actionId.toLowerCase();
            if (id.contains("register")) {
                isRegister = true;
            } else if (id.contains("login")) {
                isLogin = true;
            }
        }

        if (!isRegister && !isLogin && title != null) {
            String lower = title.toLowerCase();
            isRegister = title.contains("注册")
                    || lower.contains("register")
                    || lower.contains("sign up");
            isLogin = title.contains("登录")
                    || lower.contains("login");
        }

        if (isRegister) {
            System.out.println("[" + username + "] detected register dialog");
            doRegister(dialog, session, username);
        } else if (isLogin) {
            System.out.println("[" + username + "] detected login dialog");
            doLogin(dialog, session, username);
        } else {
            System.out.println("[" + username + "] unknown dialog type, skipped");
        }

        sendCommand(session, CONFIG.autoAuthRegisterCommand);
        System.out.println("[" + username + "] sent command: /" + CONFIG.autoAuthRegisterCommand);

        sendCommand(session, CONFIG.autoAuthLoginCommand);
        System.out.println("[" + username + "] sent command: /" + CONFIG.autoAuthLoginCommand);
    }

    private static String extractTitle(NbtMap dialog) {
        try {
            Object common = dialog.get("common");
            NbtMap commonMap = common instanceof NbtMap m ? m : null;
            NbtMap source = commonMap != null ? commonMap : dialog;

            Object title = source.get("title");
            return stringifyComponent(title);
        } catch (Exception e) {
            return null;
        }
    }

    private static String extractActionId(NbtMap dialog) {
        try {
            Object actions = dialog.get("actions");
            if (actions instanceof List<?> list && !list.isEmpty()) {
                Object first = list.get(0);
                if (first instanceof NbtMap firstMap) {
                    Object action = firstMap.get("action");
                    if (action instanceof NbtMap actionMap) {
                        Object id = actionMap.get("id");
                        return id == null ? null : String.valueOf(id);
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private static String stringifyComponent(Object node) {
        if (node == null) return null;
        if (node instanceof String s) return s;
        if (node instanceof NbtMap map) {
            for (String key : new String[]{"text", "literal", "translate"}) {
                if (map.containsKey(key)) {
                    return String.valueOf(map.get(key));
                }
            }
            if (map.containsKey("contents")) {
                return stringifyComponent(map.get("contents"));
            }
            if (map.containsKey("extra")) {
                Object extra = map.get("extra");
                if (extra instanceof List<?> list) {
                    StringBuilder sb = new StringBuilder();
                    for (Object o : list) {
                        String p = stringifyComponent(o);
                        if (p != null) sb.append(p);
                    }
                    return sb.toString();
                }
            }
        }
        return String.valueOf(node);
    }

    private static void doRegister(NbtMap dialog, ClientNetworkSession session, String username) {
        try {
            NbtMap payload = NbtMap.builder()
                    .putString("password", CONFIG.authPassword)
                    .putString("confirm", CONFIG.authPassword)
                    .build();

            session.send(new ServerboundCustomClickActionPacket(
                    Key.key("authme", "prejoin-register/submit"),
                    payload
            ));
            System.out.println("[" + username + "] sent register action");
        } catch (Exception e) {
            System.err.println("register failed: " + e.getMessage());
        }
    }

    private static void doLogin(NbtMap dialog, ClientNetworkSession session, String username) {
        try {
            NbtMap payload = NbtMap.builder()
                    .putString("password", CONFIG.authPassword)
                    .build();

            session.send(new ServerboundCustomClickActionPacket(
                    Key.key("authme", "prejoin-login/submit"),
                    payload
            ));
            System.out.println("[" + username + "] sent login action");
        } catch (Exception e) {
            System.err.println("login failed: " + e.getMessage());
        }
    }

    private static List<String> loadProxies() throws IOException {
        List<String> result = new ArrayList<>();
        if (!Files.exists(CONFIG.proxyFile)) {
            return result;
        }
        for (String line : Files.readAllLines(CONFIG.proxyFile, StandardCharsets.UTF_8)) {
            String s = line.trim();
            if (s.isEmpty() || s.startsWith("#")) continue;
            result.add(s);
        }
        return result;
    }

    private static ClientNetworkSession createBot(String username, String proxyStr,
                                                  AtomicBoolean loggedIn,
                                                  AtomicBoolean disconnected) {
        MinecraftProtocol protocol = new MinecraftProtocol(username);

        String[] parts = proxyStr.split(":");
        ProxyInfo proxyInfo = new ProxyInfo(
                ProxyInfo.Type.SOCKS5,
                parts[0],
                Integer.parseInt(parts[1])
        );

        ClientNetworkSession session = ClientNetworkSessionFactory.factory()
                .setAddress(CONFIG.host, CONFIG.port)
                .setProtocol(protocol)
                .setProxy(proxyInfo)
                .create();

        session.addListener(new SessionAdapter() {
            @Override
            public void packetReceived(Session session, Packet packet) {
                if (packet instanceof ClientboundLoginPacket) {
                    loggedIn.set(true);
                    System.out.println("[" + username + "] joined server "
                            + CONFIG.host + ":" + CONFIG.port + " (proxy: " + proxyStr + ")");
                } else if (packet instanceof ClientboundShowDialogGamePacket dialogPkt) {
                    handleDialog(dialogPkt, (ClientNetworkSession) session, username);
                } else if (packet instanceof ClientboundShowDialogConfigurationPacket dialogPkt) {
                    handleDialog(dialogPkt, (ClientNetworkSession) session, username);
                } else if (packet instanceof ClientboundPlayerChatPacket chat) {
                    String name = chat.getName() == null ? "" : PLAIN.serialize(chat.getName());
                    String content = chat.getUnsignedContent() != null
                            ? PLAIN.serialize(chat.getUnsignedContent())
                            : chat.getContent();
                    System.out.println("[chat][" + username + "] " + name + ": " + content);
                    handleChatText(content, (ClientNetworkSession) session, username);
                } else if (packet instanceof ClientboundSystemChatPacket sys) {
                    String content = PLAIN.serialize(sys.getContent());
                    System.out.println("[system][" + username + "] " + content);
                    handleChatText(content, (ClientNetworkSession) session, username);
                } else if (packet instanceof ClientboundDisguisedChatPacket dis) {
                    String name = PLAIN.serialize(dis.getName());
                    String msg = PLAIN.serialize(dis.getMessage());
                    System.out.println("[chat][" + username + "] " + name + ": " + msg);
                    handleChatText(msg, (ClientNetworkSession) session, username);
                }
            }

            @Override
            public void disconnected(DisconnectedEvent event) {
                disconnected.set(true);
                String reason = event.getReason() != null
                        ? PLAIN.serialize(event.getReason()) : "";
                Throwable cause = event.getCause();
                System.out.println("[" + username + "] disconnected: " + reason
                        + (cause != null ? " (" + cause.getClass().getSimpleName()
                        + ": " + cause.getMessage() + ")" : ""));
            }
        });

        session.connect();
        return session;
    }

    private static String generateRandomName() {
        StringBuilder sb = new StringBuilder(CONFIG.namePrefix);
        for (int i = 0; i < CONFIG.suffixLength; i++) {
            sb.append(CONFIG.chars.charAt(RANDOM.nextInt(CONFIG.chars.length())));
        }
        return sb.toString();
    }

    public static final class Config {

        final String host;
        final int port;
        final Path proxyFile;

        final String namePrefix;
        final int suffixLength;
        final String chars;

        final long chatIntervalMs;
        final List<String> chatMessages;

        final boolean autoCaptcha;

        final boolean addRandomNoise;
        final double noiseProbability;
        final String noiseChars;

        final long loginTimeoutMs;
        final int maxAttempts;
        final long retryDelayMs;

        final boolean queryServerStatus;
        final long statusQueryWaitMs;
        final long statusQueryDelayMs;

        final boolean autoAuth;
        final String authPassword;
        final String autoAuthRegisterCommand;
        final String autoAuthLoginCommand;

        final int maxJoinsPerMinute;
        final long joinIntervalSeconds;

        private Config(Properties p) {
            this.host = get(p, "host", "114.97.92.156");
            this.port = getInt(p, "port", 25565);
            this.proxyFile = Paths.get(get(p, "proxy_file", "god_proxy.txt"));

            this.namePrefix = get(p, "name_prefix", "bot");
            this.suffixLength = getInt(p, "suffix_length", 4);
            this.chars = get(p, "chars",
                    "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789");

            this.chatIntervalMs = getLong(p, "chat_interval_ms", 1000L);

            String rawMsgs = get(p, "chat_messages", "stress-test|中文测试1337_2333");
            List<String> msgs = new ArrayList<>();
            for (String s : rawMsgs.split("\\|")) {
                String t = s.trim();
                if (!t.isEmpty()) msgs.add(t);
            }
            if (msgs.isEmpty()) msgs.add("stress-test");
            this.chatMessages = List.copyOf(msgs);

            this.autoCaptcha = getBoolean(p, "auto_captcha", false);

            this.addRandomNoise = getBoolean(p, "add_random_noise", false);
            this.noiseProbability = getDouble(p, "noise_probability", 0.4D);
            this.noiseChars = get(p, "noise_chars",
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.·,、");

            this.loginTimeoutMs = getLong(p, "login_timeout_ms", 30_000L);
            this.maxAttempts = getInt(p, "max_attempts", 10);
            this.retryDelayMs = getLong(p, "retry_delay_ms", 3000L);

            this.queryServerStatus = getBoolean(p, "query_server_status", false);
            this.statusQueryWaitMs = getLong(p, "status_query_wait_ms", 2000L);
            this.statusQueryDelayMs = getLong(p, "status_query_delay_ms", 2500L);

            this.autoAuth = getBoolean(p, "auto_auth", true);
            this.authPassword = get(p, "auth_password", "MyPassword123");

            String reg = get(p, "auto_auth_register_command", "");
            if (reg.isEmpty()) {
                reg = "reg " + this.authPassword + " " + this.authPassword;
            }
            this.autoAuthRegisterCommand = reg;

            String login = get(p, "auto_auth_login_command", "");
            if (login.isEmpty()) {
                login = "login " + this.authPassword;
            }
            this.autoAuthLoginCommand = login;

            this.maxJoinsPerMinute = getInt(p, "max_joins_per_minute", 3);
            this.joinIntervalSeconds = getLong(p, "join_interval_seconds", 0L);

            if (this.suffixLength < 0) {
                System.err.println("suffix_length cannot be negative, using 0");
            }
            if (this.chars.isEmpty()) {
                throw new IllegalStateException("chars cannot be empty");
            }
            if (this.noiseProbability < 0 || this.noiseProbability > 1) {
                System.err.println("noise_probability should be between 0.0 and 1.0");
            }
        }

        static Config load(Path file) throws IOException {
            if (!Files.exists(file)) {
                try {
                    Files.writeString(file, DEFAULT_TEMPLATE, StandardCharsets.UTF_8);
                    System.out.println("Config not found, generated default config at: "
                            + file.toAbsolutePath());
                    System.out.println("Edit it and restart the program.");
                } catch (IOException e) {
                    System.err.println("Failed to generate default config: " + e.getMessage());
                    throw e;
                }
                return null;
            }

            Properties p = new Properties();
            try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                p.load(r);
            }
            Config c = new Config(p);
            System.out.println("Loaded config: " + file.toAbsolutePath());
            return c;
        }

        private static String get(Properties p, String key, String def) {
            String v = p.getProperty(key);
            if (v == null) return def;
            return v.trim();
        }

        private static int getInt(Properties p, String key, int def) {
            String v = get(p, key, "");
            if (v.isEmpty()) return def;
            try {
                return Integer.parseInt(v);
            } catch (NumberFormatException e) {
                System.err.println("Config " + key + " is not a valid int, using default " + def);
                return def;
            }
        }

        private static long getLong(Properties p, String key, long def) {
            String v = get(p, key, "");
            if (v.isEmpty()) return def;
            try {
                return Long.parseLong(v);
            } catch (NumberFormatException e) {
                System.err.println("Config " + key + " is not a valid long, using default " + def);
                return def;
            }
        }

        private static double getDouble(Properties p, String key, double def) {
            String v = get(p, key, "");
            if (v.isEmpty()) return def;
            try {
                return Double.parseDouble(v);
            } catch (NumberFormatException e) {
                System.err.println("Config " + key + " is not a valid double, using default " + def);
                return def;
            }
        }

        private static boolean getBoolean(Properties p, String key, boolean def) {
            String v = get(p, key, "");
            if (v.isEmpty()) return def;
            if ("true".equalsIgnoreCase(v)) return true;
            if ("false".equalsIgnoreCase(v)) return false;
            System.err.println("Config " + key + " is not true/false, using default " + def);
            return def;
        }

        private static final String DEFAULT_TEMPLATE = """
# ===================== 基础连接配置 =====================
# 目标服务器地址
host=114.97.92.156
# 目标服务器端口
port=25565
# 代理列表文件路径（每行一个 ip:port，以 # 开头为注释）
proxy_file=god_proxy.txt

# ===================== 随机昵称配置 =====================
# 昵称前缀
name_prefix=bot
# 昵称随机后缀长度
suffix_length=4
# 昵称随机字符集
chars=ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789

# ===================== 聊天配置 =====================
# 聊天发送间隔（毫秒）
chat_interval_ms=1000
# 聊天消息列表，多条消息使用竖线 | 分隔
chat_messages=stress-test|中文测试1337_2333

# ===================== 验证码配置 =====================
# 是否自动回应 /captcha 验证码
auto_captcha=false

# ===================== 随机噪声配置 =====================
# 是否在聊天消息中随机插入噪声字符
add_random_noise=false
# 插入噪声的概率（0.0 - 1.0）
noise_probability=0.4
# 噪声字符集
noise_chars=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.·,、

# ===================== 重试/超时配置 =====================
# 登录超时时间（毫秒）
login_timeout_ms=30000
# 单个 bot 最大尝试次数
max_attempts=10
# 重试间隔（毫秒）
retry_delay_ms=3000

# ===================== 服务器状态查询配置 =====================
# 加入前是否先查询服务器状态
query_server_status=false
# 状态查询等待响应时间（毫秒）
status_query_wait_ms=2000
# 状态查询后延迟（毫秒）
status_query_delay_ms=2500

# ===================== 自动认证配置 =====================
# 是否启用自动注册/登录
auto_auth=true
# 自动认证使用的密码
auth_password=MyPassword123
# 自动注册命令（不带前导斜杠），留空则自动使用 "reg <密码> <密码>"
auto_auth_register_command=
# 自动登录命令（不带前导斜杠），留空则自动使用 "login <密码>"
auto_auth_login_command=

# ===================== 加入限速配置 =====================
# 每分钟最多允许加入的 bot 数量，0 表示不限制
max_joins_per_minute=3
# 每加入一个 bot 后至少间隔的秒数，0 表示不限制
join_interval_seconds=0

                """;
    }
}