package server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class FileNodeServer {
    private final int port;
    private final File baseDir;

    public FileNodeServer(int port, String baseDirPath) {
        this.port = port;
        this.baseDir = new File(baseDirPath);
        if (!baseDir.exists()) baseDir.mkdirs();

        // Create the three default department directories
        for (String dept : List.of("QA", "Graphic", "Development")) {
            File deptDir = new File(baseDir, dept);
            if (!deptDir.exists()) {
                boolean created = deptDir.mkdirs();
                System.out.println((created ? "Created " : "Failed to create ") + deptDir.getAbsolutePath());
            } else {
                System.out.println("Already exists: " + deptDir.getAbsolutePath());
            }
        }
    }

    public void start() throws IOException {
        // Use fixed thread pool instead of unlimited threads
        ExecutorService threadPool = Executors.newFixedThreadPool(10); // Adjust size as needed

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setSoTimeout(5000); // Set accept timeout
            System.out.println("File Node running on port " + port);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket socket = serverSocket.accept();
                    // Configure socket before handling
                    socket.setSoTimeout(5000); // 5 second timeout
                    socket.setTcpNoDelay(true); // Disable Nagle's algorithm

                    threadPool.execute(() -> handleClient(socket));
                } catch (SocketTimeoutException e) {
                    // Normal timeout during accept, continue waiting
                } catch (IOException e) {
                    System.err.println("Accept failed: " + e.getMessage());
                }
            }
        } finally {
            threadPool.shutdown();
        }
    }

    private void handleClient(Socket socket) {
        try {
            socket.setSoTimeout(5000);

            // Initialize output stream FIRST
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();

            // Then input stream
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            // Read the action first
            String action = in.readUTF();

            // Special handling for "list" command
            if ("list".equals(action)) {
                String department = in.readUTF();
                handleListAction(out, department);
                return;
            }

            // Normal handling for other commands
            String department = in.readUTF();
            String filename = in.readUTF();

            System.out.println("[NODE] Received command: " + action + " for " + department + "/" + filename);

            switch (action) {
                case "add":
                case "edit":
                    handleAddEditAction(in, out, department, filename);
                    break;
                case "delete":
                    handleDeleteAction(out, department, filename);
                    break;
                case "fetch":
                    handleFetchAction(out, department, filename);
                    break;
                default:
                    System.out.println("[NODE] Invalid action: " + action);
                    out.writeBoolean(false);
            }
        } catch (Exception e) {
            System.out.println("[NODE] Error: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                System.out.println("[NODE] Error closing socket: " + e.getMessage());
            }
        }
    }
    private void handleListAction(ObjectOutputStream out, String department) throws IOException {
        File deptDir = new File(baseDir, department);
        List<String> files = new ArrayList<>();

        if (deptDir.exists()) {
            File[] listFiles = deptDir.listFiles();
            if (listFiles != null) {
                for (File f : listFiles) {
                    if (f.isFile()) {
                        files.add(f.getName());
                    }
                }
            }
        }

        out.writeObject(files);
        out.flush();
    }
    private void handleAddEditAction(ObjectInputStream in, ObjectOutputStream out,
                                     String department, String filename) throws Exception {
        byte[] content = (byte[]) in.readObject();
        File deptDir = new File(baseDir, department);
        if (!deptDir.exists()) {
            deptDir.mkdirs();
        }

        File file = new File(deptDir, filename);
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(content);
            out.writeBoolean(true);
            System.out.println("[NODE] File " + filename + " saved successfully");
        } catch (IOException e) {
            out.writeBoolean(false);
            System.out.println("[NODE] Error saving file: " + e.getMessage());
        }
    }

    private void handleDeleteAction(ObjectOutputStream out,
                                    String department, String filename) throws IOException {
        File file = new File(new File(baseDir, department), filename);
        boolean deleted = file.exists() && file.delete();
        out.writeBoolean(deleted);
        System.out.println("[NODE] Delete " + filename + " result: " + deleted);
    }

    private void handleFetchAction(ObjectOutputStream out,
                                   String department, String filename) throws IOException {
        System.out.println("[NODE] Fetch request for: " + department + "/" + filename);
        File deptDir = new File(baseDir, department);
        File targetFile = new File(deptDir, filename);

        // Debug output
        System.out.println("[NODE] Looking in: " + targetFile.getAbsolutePath());
        System.out.println("[NODE] File exists: " + targetFile.exists());

        if (targetFile.exists() && targetFile.isFile()) {
            try {
                byte[] data = Files.readAllBytes(targetFile.toPath());
                System.out.println("[NODE] Sending " + data.length + " bytes");
                out.writeObject(data);
            } catch (IOException e) {
                System.out.println("[NODE] Read error: " + e.getMessage());
                out.writeObject(new byte[0]);
            }
        } else {
            System.out.println("[NODE] File not found");
            out.writeObject(new byte[0]);
        }
    }

    public static void main(String[] args) {
        // Use consistent port numbers (5001-5003)
        new Thread(() -> startNode(5001, "node1")).start();
        new Thread(() -> startNode(5002, "node2")).start();
        new Thread(() -> startNode(5003, "node3")).start();
    }

    private static void startNode(int port, String name) {
        try {
            System.out.println("Starting " + name + " on port " + port);
            new FileNodeServer(port, name).start();
        } catch (IOException e) {
            System.err.println(name + " failed: " + e.getMessage());
        }
    }
}
