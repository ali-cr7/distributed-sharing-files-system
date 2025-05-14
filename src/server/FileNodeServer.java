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
            out.flush();
            System.out.println("[NODE] File " + filename + " saved successfully");
        } catch (IOException e) {
            out.writeBoolean(false);
            out.flush();
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
                out.writeBoolean(true);
                out.flush();
            } catch (IOException e) {
                System.out.println("[NODE] Read error: " + e.getMessage());
                out.writeObject(new byte[0]);
                out.writeBoolean(false);
                out.flush();
            }
        } else {
            System.out.println("[NODE] File not found");
            out.writeObject(new byte[0]);
        }
    }

    public static void main(String[] args) {
        var nodeThreads = new java.util.HashMap<Integer, Thread>();
        var activePorts = List.of(5001, 5002, 5003);

        // Start all three nodes at the beginning
        for (int i = 0; i < activePorts.size(); i++) {
            int port = activePorts.get(i);
            int nodeNum = i + 1;
            Thread t = new Thread(() -> startNode(port, "node" + nodeNum));
            t.start();
            nodeThreads.put(port, t);
            System.out.println("Node " + nodeNum + " started on port " + port);
        }

        java.util.Scanner scanner = new java.util.Scanner(System.in);

        while (true) {
            System.out.println("\n=== File Node Controller ===");
            System.out.println("1) Start node");
            System.out.println("2) Stop node");
            System.out.println("3) List active nodes");
            System.out.println("0) Exit");
            System.out.print("Choose: ");
            String choice = scanner.nextLine();

            switch (choice) {
                case "1" -> {
                    System.out.print("Enter node number (1-3): ");
                    int num = Integer.parseInt(scanner.nextLine());
                    if (num < 1 || num > 3) {
                        System.out.println("Invalid node number.");
                        continue;
                    }
                    int port = activePorts.get(num - 1);
                    if (nodeThreads.containsKey(port)) {
                        System.out.println("Node " + num + " is already running.");
                        continue;
                    }
                    Thread t = new Thread(() -> startNode(port, "node" + num));
                    t.start();
                    nodeThreads.put(port, t);
                    System.out.println("Node " + num + " started.");
                }

                case "2" -> {
                    System.out.print("Enter node number (1-3): ");
                    int num = Integer.parseInt(scanner.nextLine());
                    int port = activePorts.get(num - 1);
                    Thread t = nodeThreads.get(port);
                    if (t != null) {
                        t.interrupt();
                        nodeThreads.remove(port);
                        System.out.println("Node " + num + " stopped.");
                    } else {
                        System.out.println("Node " + num + " is not running.");
                    }
                }

                case "3" -> {
                    System.out.println("Active Nodes:");
                    for (var entry : nodeThreads.entrySet()) {
                        System.out.println(" - Node on port " + entry.getKey() + " is running.");
                    }
                    if (nodeThreads.isEmpty()) {
                        System.out.println("No active nodes.");
                    }
                }

                case "0" -> {
                    System.out.println("Shutting down all nodes...");
                    for (Thread t : nodeThreads.values()) {
                        t.interrupt();
                    }
                    return;
                }

                default -> System.out.println("Invalid choice.");
            }
        }
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
