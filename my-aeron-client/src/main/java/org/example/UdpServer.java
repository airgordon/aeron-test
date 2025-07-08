package org.example;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;

public class UdpServer {

    public static void runServer(int port) {
        Runnable r = () -> {
            try {
                DatagramSocket ds = new DatagramSocket(port);
                byte[] buf = new byte[500];
                while (true) {
                    DatagramPacket pack = new DatagramPacket(buf, 500);
                    ds.receive(pack);
                    System.out.println("got UDP from " + pack.getAddress() + " " + pack.getPort() + " " + pack.getLength() + " '" + new String(pack.getData(), 0, pack.getLength(), StandardCharsets.US_ASCII) + "'");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        new Thread(r).start();
    }
}
