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
    private static final Random RANDOM = new Random();
    private static final PlainTextComponentSerializer PLAIN = PlainTextComponentSerializer.plainText();
    private static final Pattern CAPTCHA_PATTERN = Pattern.compile("/captcha\\s+(\\S+)");
    private static final Object JOIN_LOCK = new Object();
    private static final ArrayDeque<Long> RECENT_JOINS = new ArrayDeque<>();

    private static Config CONFIG;
    private static long lastJoinAt = 0L;

    public static void main(String[] args) throws Exception {
        CONFIG = Config.load(CONFIG_FILE);
        if (CONFIG == null) return;

        List<String> proxies = loadProxies();
        if (proxies.isEmpty()) {
            System.err.println("Proxy list is empty, check " + CONFIG.proxyFile.toAbsolutePath());
            return;
        }

        int total = proxies.size();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            final int index = i + 1;
            final String proxy = proxies.get(i);
            Thread t = new Thread(() -> runBot(index, total, proxy), "bot-" + index);
            t.start();
            threads.add(t);
        }

        System.out.println("Started " + threads.size() + " bot threads");
        printRateLimitConfig();
        for (Thread t : threads) t.join();
    }

    private static void printRateLimitConfig() {
        String perMinute = CONFIG.maxJoinsPerMinute > 0 ? CONFIG.maxJoinsPerMinute + " per minute" : "unlimited";
        String interval = CONFIG.joinIntervalSeconds > 0 ? CONFIG.joinIntervalSeconds + "s per join" : "unlimited";
        System.out.println("Rate limit: perMinute=" + perMinute + ", interval=" + interval);
    }

    private static void waitForJoinSlot() throws InterruptedException {
        final long minuteMs = 60_000L;
        final long intervalMs = CONFIG.joinIntervalSeconds * 1000L;
        if (CONFIG.maxJoinsPerMinute <= 0 && intervalMs <= 0) return;

        while (true) {
            long waitMs;
            synchronized (JOIN_LOCK) {
                long now = System.currentTimeMillis();
                if (CONFIG.maxJoinsPerMinute > 0) {
                    while (!RECENT_JOINS.isEmpty() && now - RECENT_JOINS.peekFirst() >= minuteMs)
                        RECENT_JOINS.pollFirst();
                }
                long waitByInterval = (intervalMs > 0 && lastJoinAt > 0) ? Math.max(0, lastJoinAt + intervalMs - now) : 0;
                long waitByRate = (CONFIG.maxJoinsPerMinute > 0 && RECENT_JOINS.size() >= CONFIG.maxJoinsPerMinute)
                        ? Math.max(0, RECENT_JOINS.peekFirst() + minuteMs - now) : 0;
                waitMs = Math.max(waitByInterval, waitByRate);

                if (waitMs <= 0) {
                    long stamp = System.currentTimeMillis();
                    if (CONFIG.maxJoinsPerMinute > 0) RECENT_JOINS.addLast(stamp);
                    lastJoinAt = stamp;
                    return;
                }
            }
            Thread.sleep(Math.min(waitMs, 500L));
        }
    }

    private static void runBot(int index, int total, String proxy) {
        String tag = "[" + index + "/" + total + "] ";
        for (int attempt = 1; attempt <= CONFIG.maxAttempts; attempt++) {
            if (CONFIG.queryServerStatus) {
                queryServerStatus(proxy);
                try { Thread.sleep(CONFIG.statusQueryDelayMs); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }

            String username = generateRandomName();
            AtomicBoolean loggedIn = new AtomicBoolean(false);
            AtomicBoolean disconnected = new AtomicBoolean(false);

            try {
                waitForJoinSlot();
                ClientNetworkSession session = createBot(username, proxy, loggedIn, disconnected);
                System.out.println(tag + "attempt #" + attempt + " starting bot: " + username + " (proxy: " + proxy + ")");

                long deadline = System.currentTimeMillis() + CONFIG.loginTimeoutMs;
                while (!loggedIn.get() && !disconnected.get() && System.currentTimeMillis() < deadline)
                    Thread.sleep(100);

                if (!loggedIn.get()) {
                    System.out.println(tag + "attempt #" + attempt + " failed (not logged in), retrying");
                    safeDisconnect(session);
                    Thread.sleep(CONFIG.retryDelayMs);
                    continue;
                }

                Thread.sleep(1000);
                int msgIndex = 0;
                while (!disconnected.get() && session.isConnected()) {
                    sendAny(session, CONFIG.chatMessages.get(msgIndex));
                    msgIndex = (msgIndex + 1) % CONFIG.chatMessages.size();

                    long slept = 0;
                    while (slept < CONFIG.chatIntervalMs && !disconnected.get() && session.isConnected()) {
                        long step = Math.min(100, CONFIG.chatIntervalMs - slept);
                        Thread.sleep(step);
                        slept += step;
                    }
                }

                System.out.println(tag + "bot " + username + " disconnected, attempting #" + (attempt + 1));
                safeDisconnect(session);
                Thread.sleep(CONFIG.retryDelayMs);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                System.err.println(tag + "attempt #" + attempt + " error: " + e.getMessage());
                try { Thread.sleep(CONFIG.retryDelayMs); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
            }
        }
        System.out.println(tag + "proxy " + proxy + " failed after " + CONFIG.maxAttempts + " attempts");
    }

    private static void queryServerStatus(String proxy) {
        AtomicReference<ClientNetworkSession> ref = new AtomicReference<>();
        AtomicBoolean got = new AtomicBoolean(false);
        try {
            String[] parts = proxy.split(":");
            ClientNetworkSession s = ClientNetworkSessionFactory.factory()
                    .setAddress(CONFIG.host, CONFIG.port)
                    .setProtocol(new MinecraftProtocol())
                    .setProxy(new ProxyInfo(ProxyInfo.Type.SOCKS5, parts[0], Integer.parseInt(parts[1])))
                    .create();
            ref.set(s);
            s.addListener(new SessionAdapter() {
                @Override public void packetReceived(Session session, Packet packet) {
                    if (!(packet instanceof ClientboundStatusResponsePacket resp)) return;
                    got.set(true);
                    var info = resp.parseInfo();
                    String motd = info.getDescription() == null ? "" : PLAIN.serialize(info.getDescription());
                    var ver = info.getVersionInfo();
                    var players = info.getPlayerInfo();
                    System.out.println("[status][" + proxy + "]");
                    System.out.println("  MOTD: " + motd);
                    if (ver != null)
                        System.out.println("  version: " + ver.getVersionName() + " (protocol " + ver.getProtocolVersion() + ")");
                    if (players != null)
                        System.out.println("  players: " + players.getOnlinePlayers() + "/" + players.getMaxPlayers());
                }
            });
            s.connect();
            Thread.sleep(300);
            s.send(new ServerboundStatusRequestPacket());

            long deadline = System.currentTimeMillis() + CONFIG.statusQueryWaitMs;
            while (!got.get() && System.currentTimeMillis() < deadline) Thread.sleep(50);
            if (!got.get()) System.out.println("[status][" + proxy + "] no response");

        } catch (Exception e) {
            System.err.println("[status][" + proxy + "] query failed: " + e.getMessage());
        } finally {
            ClientNetworkSession s = ref.get();
            if (s != null) try { if (s.isConnected()) s.disconnect("status query done"); } catch (Exception ignored) {}
        }
    }

    private static void safeDisconnect(ClientNetworkSession session) {
        try { if (session != null && session.isConnected()) session.disconnect("retry"); }
        catch (Exception ignored) {}
    }

    private static void sendAny(ClientNetworkSession session, String text) {
        if (!session.isConnected()) return;
        if (text.startsWith("/")) { sendCommand(session, text.substring(1)); return; }
        try {
            session.send(new ServerboundChatPacket(
                    CONFIG.addRandomNoise ? addNoise(text) : text,
                    System.currentTimeMillis(), 0L, null, 0, new BitSet(), 0));
        } catch (Exception e) {
            System.err.println("sendChat failed: " + e.getMessage());
        }
    }

    private static void sendCommand(ClientNetworkSession session, String command) {
        if (!session.isConnected()) return;
        try { session.send(new ServerboundChatCommandPacket(command)); }
        catch (Exception e) { System.err.println("sendCommand failed: " + e.getMessage()); }
    }

    private static String addNoise(String input) {
        StringBuilder sb = new StringBuilder(input.length() * 2);
        for (int i = 0; i < input.length(); i++) {
            sb.append(input.charAt(i));
            if (RANDOM.nextDouble() < CONFIG.noiseProbability && !CONFIG.noiseChars.isEmpty())
                sb.append(CONFIG.noiseChars.charAt(RANDOM.nextInt(CONFIG.noiseChars.length())));
        }
        return sb.toString();
    }

    private static void handleChatText(String text, ClientNetworkSession session, String username) {
        if (!CONFIG.autoCaptcha || text == null) return;
        Matcher m = CAPTCHA_PATTERN.matcher(text);
        if (!m.find()) return;
        String code = m.group(1);
        System.out.println("[" + username + "] captcha detected, sending /captcha " + code);
        sendCommand(session, "captcha " + code);
    }

    private static void processDialogNbt(NbtMap dialog, ClientNetworkSession session, String username) {
        System.out.println("[" + username + "] dialog nbt: " + dialog);
        if (!CONFIG.autoAuth) return;

        String title = extractTitle(dialog);
        String actionId = extractActionId(dialog);
        System.out.println("[" + username + "] dialog title: " + title + ", action id: " + actionId);

        boolean isRegister = false, isLogin = false;
        if (actionId != null) {
            String id = actionId.toLowerCase();
            if (id.contains("register")) isRegister = true;
            else if (id.contains("login")) isLogin = true;
        }
        if (!isRegister && !isLogin && title != null) {
            String lower = title.toLowerCase();
            isRegister = title.contains("注册") || lower.contains("register") || lower.contains("sign up");
            isLogin = title.contains("登录") || lower.contains("login");
        }

        if (isRegister) {
            System.out.println("[" + username + "] detected register dialog");
            sendClickAction(session, username, "prejoin-register/submit",
                    NbtMap.builder()
                            .putString("password", CONFIG.authPassword)
                            .putString("confirm", CONFIG.authPassword)
                            .build());
        } else if (isLogin) {
            System.out.println("[" + username + "] detected login dialog");
            sendClickAction(session, username, "prejoin-login/submit",
                    NbtMap.builder().putString("password", CONFIG.authPassword).build());
        } else {
            System.out.println("[" + username + "] unknown dialog type, skipped");
        }

        sendCommand(session, CONFIG.autoAuthRegisterCommand);
        System.out.println("[" + username + "] sent command: /" + CONFIG.autoAuthRegisterCommand);
        sendCommand(session, CONFIG.autoAuthLoginCommand);
        System.out.println("[" + username + "] sent command: /" + CONFIG.autoAuthLoginCommand);
    }

    private static void sendClickAction(ClientNetworkSession session, String username,
                                        String path, NbtMap payload) {
        try {
            session.send(new ServerboundCustomClickActionPacket(Key.key("authme", path), payload));
            System.out.println("[" + username + "] sent " + path + " action");
        } catch (Exception e) {
            System.err.println(path + " failed: " + e.getMessage());
        }
    }

    private static String extractTitle(NbtMap dialog) {
        try {
            Object common = dialog.get("common");
            NbtMap source = common instanceof NbtMap m ? m : dialog;
            return stringifyComponent(source.get("title"));
        } catch (Exception e) {
            return null;
        }
    }

    private static String extractActionId(NbtMap dialog) {
        try {
            Object actions = dialog.get("actions");
            if (actions instanceof List<?> list && !list.isEmpty()
                    && list.get(0) instanceof NbtMap first
                    && first.get("action") instanceof NbtMap action) {
                Object id = action.get("id");
                return id == null ? null : String.valueOf(id);
            }
        } catch (Exception ignored) {}
        return null;
    }

    private static String stringifyComponent(Object node) {
        if (node == null) return null;
        if (node instanceof String s) return s;
        if (node instanceof NbtMap map) {
            for (String key : new String[]{"text", "literal", "translate"}) {
                if (map.containsKey(key)) return String.valueOf(map.get(key));
            }
            if (map.containsKey("contents")) return stringifyComponent(map.get("contents"));
            if (map.get("extra") instanceof List<?> extra) {
                StringBuilder sb = new StringBuilder();
                for (Object o : extra) {
                    String p = stringifyComponent(o);
                    if (p != null) sb.append(p);
                }
                return sb.toString();
            }
        }
        return String.valueOf(node);
    }

    private static List<String> loadProxies() throws IOException {
        if (!Files.exists(CONFIG.proxyFile)) return List.of();
        List<String> result = new ArrayList<>();
        for (String line : Files.readAllLines(CONFIG.proxyFile, StandardCharsets.UTF_8)) {
            String s = line.trim();
            if (!s.isEmpty() && !s.startsWith("#")) result.add(s);
        }
        return result;
    }

    private static ClientNetworkSession createBot(String username, String proxy,
                                                  AtomicBoolean loggedIn,
                                                  AtomicBoolean disconnected) {
        String[] parts = proxy.split(":");
        ClientNetworkSession session = ClientNetworkSessionFactory.factory()
                .setAddress(CONFIG.host, CONFIG.port)
                .setProtocol(new MinecraftProtocol(username))
                .setProxy(new ProxyInfo(ProxyInfo.Type.SOCKS5, parts[0], Integer.parseInt(parts[1])))
                .create();

        session.addListener(new SessionAdapter() {
            @Override public void packetReceived(Session s, Packet packet) {
                try {
                    if (packet instanceof ClientboundLoginPacket) {
                        loggedIn.set(true);
                        System.out.println("[" + username + "] joined server " + CONFIG.host + ":" + CONFIG.port
                                + " (proxy: " + proxy + ")");
                    } else if (packet instanceof ClientboundShowDialogGamePacket d) {
                        if (d.getDialog().isId()) {
                            System.out.println("[" + username + "] dialog reference (id="
                                    + d.getDialog().id() + "), content unavailable");
                        } else {
                            processDialogNbt(d.getDialog().custom(), (ClientNetworkSession) s, username);
                        }
                    } else if (packet instanceof ClientboundShowDialogConfigurationPacket d) {
                        processDialogNbt(d.getDialog(), (ClientNetworkSession) s, username);
                    } else if (packet instanceof ClientboundPlayerChatPacket c) {
                        String name = c.getName() == null ? "" : PLAIN.serialize(c.getName());
                        String content = c.getUnsignedContent() != null
                                ? PLAIN.serialize(c.getUnsignedContent()) : c.getContent();
                        System.out.println("[chat][" + username + "] " + name + ": " + content);
                        handleChatText(content, (ClientNetworkSession) s, username);
                    } else if (packet instanceof ClientboundSystemChatPacket c) {
                        String content = PLAIN.serialize(c.getContent());
                        System.out.println("[system][" + username + "] " + content);
                        handleChatText(content, (ClientNetworkSession) s, username);
                    } else if (packet instanceof ClientboundDisguisedChatPacket c) {
                        String name = PLAIN.serialize(c.getName());
                        String msg = PLAIN.serialize(c.getMessage());
                        System.out.println("[chat][" + username + "] " + name + ": " + msg);
                        handleChatText(msg, (ClientNetworkSession) s, username);
                    }
                } catch (Exception e) {
                    System.err.println("packet handling failed: " + e.getMessage());
                }
            }

            @Override public void disconnected(DisconnectedEvent e) {
                disconnected.set(true);
                String reason = e.getReason() != null ? PLAIN.serialize(e.getReason()) : "";
                Throwable cause = e.getCause();
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
        for (int i = 0; i < CONFIG.suffixLength; i++)
            sb.append(CONFIG.chars.charAt(RANDOM.nextInt(CONFIG.chars.length())));
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
            this.host = str(p, "host");
            this.port = i(p, "port");
            this.proxyFile = Paths.get(str(p, "proxy_file"));
            this.namePrefix = str(p, "name_prefix");
            this.suffixLength = i(p, "suffix_length");
            this.chars = str(p, "chars");

            this.chatIntervalMs = l(p, "chat_interval_ms");
            List<String> msgs = new ArrayList<>();
            for (String s : str(p, "chat_messages").split("\\|")) {
                String t = s.trim();
                if (!t.isEmpty()) msgs.add(t);
            }
            if (msgs.isEmpty()) msgs.add("stress-test");
            this.chatMessages = List.copyOf(msgs);

            this.autoCaptcha = b(p, "auto_captcha");
            this.addRandomNoise = b(p, "add_random_noise");
            this.noiseProbability = d(p, "noise_probability");
            this.noiseChars = str(p, "noise_chars");
            this.loginTimeoutMs = l(p, "login_timeout_ms");
            this.maxAttempts = i(p, "max_attempts");
            this.retryDelayMs = l(p, "retry_delay_ms");
            this.queryServerStatus = b(p, "query_server_status");
            this.statusQueryWaitMs = l(p, "status_query_wait_ms");
            this.statusQueryDelayMs = l(p, "status_query_delay_ms");
            this.autoAuth = b(p, "auto_auth");
            this.authPassword = str(p, "auth_password");

            String reg = p.getProperty("auto_auth_register_command", "").trim();
            this.autoAuthRegisterCommand = reg.isEmpty()
                    ? "reg " + authPassword + " " + authPassword : reg;
            String login = p.getProperty("auto_auth_login_command", "").trim();
            this.autoAuthLoginCommand = login.isEmpty() ? "login " + authPassword : login;

            this.maxJoinsPerMinute = i(p, "max_joins_per_minute");
            this.joinIntervalSeconds = l(p, "join_interval_seconds");

            if (suffixLength < 0) System.err.println("suffix_length cannot be negative");
            if (chars.isEmpty()) throw new IllegalStateException("chars cannot be empty");
            if (noiseProbability < 0 || noiseProbability > 1)
                System.err.println("noise_probability should be between 0.0 and 1.0");
        }

        static Config load(Path file) throws IOException {
            if (!Files.exists(file)) {
                Files.writeString(file, DEFAULT_TEMPLATE, StandardCharsets.UTF_8);
                System.out.println("Config not found, generated default config at: " + file.toAbsolutePath());
                System.out.println("Edit it and restart the program.");
                return null;
            }
            Properties p = new Properties();
            try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) { p.load(r); }
            System.out.println("Loaded config: " + file.toAbsolutePath());
            return new Config(p);
        }

        private static String str(Properties p, String k) {
            String v = p.getProperty(k);
            if (v == null) throw new IllegalStateException("Missing config key: " + k);
            return v.trim();
        }
        private static int i(Properties p, String k) {
            try { return Integer.parseInt(str(p, k)); }
            catch (NumberFormatException e) { throw new IllegalStateException("Config " + k + " is not a valid int"); }
        }
        private static long l(Properties p, String k) {
            try { return Long.parseLong(str(p, k)); }
            catch (NumberFormatException e) { throw new IllegalStateException("Config " + k + " is not a valid long"); }
        }
        private static double d(Properties p, String k) {
            try { return Double.parseDouble(str(p, k)); }
            catch (NumberFormatException e) { throw new IllegalStateException("Config " + k + " is not a valid double"); }
        }
        private static boolean b(Properties p, String k) {
            String v = str(p, k);
            if ("true".equalsIgnoreCase(v)) return true;
            if ("false".equalsIgnoreCase(v)) return false;
            throw new IllegalStateException("Config " + k + " is not true/false: " + v);
        }

        private static final String DEFAULT_TEMPLATE = """
# ===================== 基础连接配置 =====================
# 目标服务器地址
host=mc.hypixel.net
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