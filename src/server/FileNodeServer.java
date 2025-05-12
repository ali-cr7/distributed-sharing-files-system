package server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;

class FileNodeServer {
    private final int port;
    private final File baseDir;

    public FileNodeServer(int port, String baseDirPath) {
        this.port = port;
        this.baseDir = new File(baseDirPath);
        if (!baseDir.exists()) baseDir.mkdirs();
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("File Node running on port " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(() -> handleClient(socket)).start();
            }
        }
    }

    private void handleClient(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            String action = in.readUTF();
            String department = in.readUTF();
            String filename = in.readUTF();
            byte[] content = null;
            if (!action.equals("fetch")) {
                content = (byte[]) in.readObject();
            }

            File deptDir = new File(baseDir, department);
            if (!deptDir.exists()) deptDir.mkdirs();
            File file = new File(deptDir, filename);

            switch (action) {
                case "add":
                case "edit":
                    try (FileOutputStream fos = new FileOutputStream(file)) {
                        fos.write(content);
                    }
                    out.writeBoolean(true);
                    break;
                case "delete":
                    out.writeBoolean(file.exists() && file.delete());
                    break;
                case "fetch":
                    if (file.exists()) {
                        byte[] data = Files.readAllBytes(file.toPath());
                        out.writeObject(data);
                    } else {
                        out.writeObject(null);
                    }
                    break;
                default:
                    out.writeBoolean(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Usage: java FileNodeServer <port> <storage_directory>");
            return;
        }
        int port = Integer.parseInt(args[0]);
        String storageDir = args[1];
        new FileNodeServer(port, storageDir).start();
    }
}
