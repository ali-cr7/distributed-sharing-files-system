package server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

class FileNodeServer {
    private final int port;
    private final File baseDir;
    private volatile boolean isOnline = true;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger artificialLoad = new AtomicInteger(0);
    private final Set<Socket> loadTestSockets = Collections.synchronizedSet(new HashSet<>());

    // Define NodeInfo as a nested record
    public record NodeInfo(Thread thread, FileNodeServer server, Set<Thread> loadThreads) {}

    public FileNodeServer(int port, String baseDirPath) {
        this.port = port;
        this.baseDir = new File(baseDirPath);
        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }

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

    private void handleClient(Socket socket) {
        activeConnections.incrementAndGet();
        try {
            socket.setSoTimeout(5000);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            while (!socket.isClosed()) {
                try {
                    String action = in.readUTF();

                    if ("ping".equals(action)) {
                        // Add socket to load test set if it's a ping connection
                        loadTestSockets.add(socket);
                        // Apply artificial load only to ping operations
                        int currentArtificialLoad = artificialLoad.get();
                        if (currentArtificialLoad > 0) {
                            try {
                                Thread.sleep(currentArtificialLoad * 10L);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        out.writeUTF("pong");
                        out.flush();
                        continue;
                    }

                    if ("getLoad".equals(action)) {
                        // Return only the number of load test connections
                        int loadTestCount = loadTestSockets.size();
                        out.writeInt(loadTestCount);
                        out.flush();
                        return;
                    }

                    String department = in.readUTF();
                    String filename = in.readUTF();
                    System.out.println("[NODE] Received command: " + action + " for " + department + "/" + filename);

                    switch (action) {
                        case "list" -> handleListAction(out, department);
                        case "add", "edit" -> handleAddEditAction(in, out, department, filename);
                        case "delete" -> handleDeleteAction(out, department, filename);
                        case "fetch" -> handleFetchAction(out, department, filename);
                        default -> {
                            System.out.println("[NODE] Invalid action: " + action);
                            out.writeBoolean(false);
                        }
                    }
                } catch (EOFException e) {
                    System.out.println("[NODE] Client disconnected normally");
                    break;
                } catch (SocketTimeoutException e) {
                    continue;
                } catch (IOException e) {
                    if (!socket.isClosed()) {
                        System.out.println("[NODE] Client connection error: " + e.getMessage());
                    }
                    break;
                } catch (Exception e) {
                    System.out.println("[NODE] Unexpected error: " + e.getMessage());
                    break;
                }
            }
        } catch (IOException e) {
            if (!socket.isClosed()) {
                System.out.println("[NODE] Initial connection error: " + e.getMessage());
            }
        } finally {
            loadTestSockets.remove(socket);
            activeConnections.decrementAndGet();
            try {
                if (!socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                // Ignore close errors
            }
        }
    }

    private void handleListAction(ObjectOutputStream out, String department) throws IOException {
        try {
            File deptDir = new File(baseDir, department);
            List<String> files = new ArrayList<>();

            System.out.println("[NODE] Listing files in directory: " + deptDir.getAbsolutePath());
            
            if (deptDir.exists()) {
                File[] listFiles = deptDir.listFiles();
                if (listFiles != null) {
                    for (File f : listFiles) {
                        if (f.isFile()) {
                            files.add(f.getName());
                            System.out.println("[NODE] Found file: " + f.getName());
                        }
                    }
                } else {
                    System.out.println("[NODE] Directory is empty or cannot be read: " + deptDir.getAbsolutePath());
                }
            } else {
                System.out.println("[NODE] Department directory does not exist: " + deptDir.getAbsolutePath());
                deptDir.mkdirs();
            }

            System.out.println("[NODE] Sending " + files.size() + " files to client");
            out.writeObject(files);
            out.flush();
        } catch (Exception e) {
            System.err.println("[NODE] Error in handleListAction: " + e.getMessage());
            e.printStackTrace();
            out.writeObject(new ArrayList<String>());
            out.flush();
        }
    }

    private void handleAddEditAction(ObjectInputStream in, ObjectOutputStream out,
                                     String department, String filename) throws Exception {
        try {
            System.out.println("[NODE] Starting " + (new File(new File(baseDir, department), filename).exists() ? "edit" : "add") + 
                             " operation for " + department + "/" + filename);
            
            byte[] content = (byte[]) in.readObject();
            if (content == null) {
                System.out.println("[NODE] Received null content for file operation");
                out.writeBoolean(false);
                out.flush();
                return;
            }

            File deptDir = new File(baseDir, department);
            if (!deptDir.exists()) {
                boolean created = deptDir.mkdirs();
                System.out.println("[NODE] " + (created ? "Created" : "Failed to create") + " directory: " + deptDir.getAbsolutePath());
            }

            File file = new File(deptDir, filename);
            try (FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(content);
                out.writeBoolean(true);
                out.flush();
                System.out.println("[NODE] File " + filename + " saved successfully");
            } catch (IOException e) {
                System.err.println("[NODE] Error writing file " + filename + ": " + e.getMessage());
                out.writeBoolean(false);
                out.flush();
            }
        } catch (Exception e) {
            System.err.println("[NODE] Error in handleAddEditAction: " + e.getMessage());
            e.printStackTrace();
            try {
                out.writeBoolean(false);
                out.flush();
            } catch (IOException ex) {
                System.err.println("[NODE] Error sending failure response: " + ex.getMessage());
            }
        }
    }

    private void handleDeleteAction(ObjectOutputStream out, String department, String filename) throws IOException {
        File file = new File(new File(baseDir, department), filename);
        boolean deleted = file.exists() && file.delete();
        out.writeBoolean(deleted);
        System.out.println("[NODE] Delete " + filename + " result: " + deleted);
    }

    private void handleFetchAction(ObjectOutputStream out, String department, String filename) throws IOException {
        System.out.println("[NODE] Fetch request for: " + department + "/" + filename);
        File deptDir = new File(baseDir, department);
        File targetFile = new File(deptDir, filename);

        if (targetFile.exists() && targetFile.isFile()) {
            try {
                byte[] data = Files.readAllBytes(targetFile.toPath());
                System.out.println("[NODE] Sending " + data.length + " bytes");
                out.writeObject(data);
                out.writeBoolean(true);
                out.flush();
            } catch (IOException e) {
                System.err.println("[NODE] Error reading file: " + e.getMessage());
                out.writeObject(new byte[0]);
            }
        } else {
            System.out.println("[NODE] File not found");
            out.writeObject(new byte[0]);
        }
    }

    public int getCurrentLoad() {
        return loadTestSockets.size();
    }

    public void addArtificialLoad(int amount) {
        artificialLoad.addAndGet(amount);
    }

    public void removeArtificialLoad(int amount) {
        artificialLoad.updateAndGet(current -> Math.max(0, current - amount));
    }

    public void clearArtificialLoad() {
        artificialLoad.set(0);
    }

    public int getArtificialLoad() {
        return artificialLoad.get();
    }

    public void start() throws IOException {
        ExecutorService threadPool = Executors.newFixedThreadPool(10);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setSoTimeout(5000);
            System.out.println("File Node running on port " + port);

            while (!Thread.currentThread().isInterrupted()) {
                if (!isOnline) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                try {
                    Socket socket = serverSocket.accept();
                    socket.setSoTimeout(5000);
                    socket.setTcpNoDelay(true);
                    threadPool.execute(() -> handleClient(socket));
                } catch (SocketTimeoutException e) {
                    // Timeout is expected, continue looping
                } catch (IOException e) {
                    System.err.println("Accept failed: " + e.getMessage());
                }
            }
        } finally {
            threadPool.shutdown();
        }
    }

    public static void main(String[] args) {
        Map<Integer, NodeInfo> nodeMap = new ConcurrentHashMap<>();
        List<Integer> ports = List.of(5001, 5002, 5003);

        // Initialize nodes
        for (int i = 0; i < ports.size(); i++) {
            int port = ports.get(i);
            String name = "node" + (i + 1);
            FileNodeServer server = new FileNodeServer(port, name);
            Thread t = new Thread(() -> {
                try {
                    server.start();
                } catch (IOException e) {
                    System.out.println("[NODE] Start failed: " + e.getMessage());
                }
            }, "Node-" + port);
            t.start();
            nodeMap.put(port, new NodeInfo(t, server, ConcurrentHashMap.newKeySet()));
            System.out.println("Node " + name + " started on port " + port);
        }

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n=== File Node Controller ===");
            System.out.println("1) Start node");
            System.out.println("2) Stop node");
            System.out.println("3) List active nodes");

            System.out.println("0) Exit");
            System.out.print("Choose: ");

            try {
                String choice = scanner.nextLine();

                switch (choice) {
                    case "1" -> startNode(scanner, nodeMap, ports);
                    case "2" -> stopNode(scanner, nodeMap, ports);
                    case "3" -> listNodes(nodeMap);

                    case "0" -> {
                        shutdownAll(nodeMap);
                        return;
                    }
                    default -> System.out.println("Invalid choice.");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }

    private static void startNode(Scanner scanner, Map<Integer, FileNodeServer.NodeInfo> nodeMap, List<Integer> ports) {
        System.out.print("Enter node number (1-3): ");
        int num = Integer.parseInt(scanner.nextLine());
        if (num < 1 || num > 3) {
            System.out.println("Invalid node number.");
            return;
        }
        int port = ports.get(num - 1);
        if (nodeMap.containsKey(port)) {
            System.out.println("Node " + num + " is already running.");
            return;
        }
        String name = "node" + num;
        FileNodeServer server = new FileNodeServer(port, name);
        Thread t = new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                System.out.println("[NODE] Start failed: " + e.getMessage());
            }
        }, "Node-" + port);
        t.start();
        nodeMap.put(port, new NodeInfo(t, server, ConcurrentHashMap.newKeySet()));
        System.out.println("Node " + num + " started.");
    }

    private static void stopNode(Scanner scanner, Map<Integer, FileNodeServer.NodeInfo> nodeMap, List<Integer> ports) {
        System.out.print("Enter node number (1-3): ");
        int num = Integer.parseInt(scanner.nextLine());
        int port = ports.get(num - 1);
        FileNodeServer.NodeInfo info = nodeMap.get(port);
        if (info != null) {
            info.loadThreads().forEach(Thread::interrupt);
            info.loadThreads().clear();
            info.thread().interrupt();
            nodeMap.remove(port);
            System.out.println("Node " + num + " stopped.");
        } else {
            System.out.println("Node " + num + " is not running.");
        }
    }

    private static void listNodes(Map<Integer, FileNodeServer.NodeInfo> nodeMap) {
        if (nodeMap.isEmpty()) {
            System.out.println("No active nodes.");
        } else {
            System.out.println("Active Nodes:");
            nodeMap.forEach((port, info) -> {
                int loadTestCount = info.server().loadTestSockets.size();
                int activeConnCount = info.server().activeConnections.get();
                System.out.printf(" - Node on port %d: %s%n", port,
                        info.thread().isAlive() ? "RUNNING" : "STOPPED");
                System.out.printf("   Load: %d (Real: %d)%n",
                        loadTestCount, activeConnCount);
            });
        }
    }

    private static void shutdownAll(Map<Integer, FileNodeServer.NodeInfo> nodeMap) {
        System.out.println("Shutting down all nodes...");
        nodeMap.values().forEach(info -> {
            info.loadThreads().forEach(Thread::interrupt);
            info.thread().interrupt();
        });
    }
}