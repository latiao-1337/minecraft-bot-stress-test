package me.latiao;

import org.cloudburstmc.nbt.NbtMap;
import org.geysermc.mcprotocollib.network.Session;
import org.geysermc.mcprotocollib.network.event.session.SessionAdapter;
import org.geysermc.mcprotocollib.network.factory.ClientNetworkSessionFactory;
import org.geysermc.mcprotocollib.network.packet.Packet;
import org.geysermc.mcprotocollib.network.session.ClientNetworkSession;
import org.geysermc.mcprotocollib.protocol.MinecraftProtocol;
import org.geysermc.mcprotocollib.protocol.packet.configuration.clientbound.ClientboundShowDialogConfigurationPacket;
import org.geysermc.mcprotocollib.protocol.packet.ingame.clientbound.ClientboundShowDialogGamePacket;

public class DialogDump {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 25565;
    private static final String USERNAME = "test123123";

    public static void main(String[] args) throws Exception {
        MinecraftProtocol protocol = new MinecraftProtocol(USERNAME);

        ClientNetworkSession session = ClientNetworkSessionFactory.factory()
                .setAddress(HOST, PORT)
                .setProtocol(protocol)
                .create();

        session.addListener(new SessionAdapter() {
            @Override
            public void packetReceived(Session session, Packet packet) {
                if (packet instanceof ClientboundShowDialogGamePacket p) {
                    System.out.println("========== [game] Dialog ==========");
                    if (p.getDialog().isId()) {
                        System.out.println("(引用 ID=" + p.getDialog().id() + ")");
                    } else {
                        NbtMap dialog = p.getDialog().custom();
                        System.out.println(dialog);
                    }
                    System.out.println("===================================");
                } else if (packet instanceof ClientboundShowDialogConfigurationPacket p) {
                    System.out.println("========== [config] Dialog ==========");
                    System.out.println(p.getDialog());
                    System.out.println("=====================================");
                }
            }
        });

        session.connect();
        System.out.println("已连接 " + HOST + ":" + PORT + "，等待 Dialog 包...");
        Thread.sleep(Long.MAX_VALUE);
    }
}