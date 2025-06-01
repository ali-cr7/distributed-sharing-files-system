package server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class   FileNodeServer {
    private final int port;
    private final File baseDir;
    private volatile boolean isOnline = true;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final Set<Socket> loadTestSockets = Collections.synchronizedSet(new HashSet<>());
    private final int SOCKET_TIMEOUT = 30000; // 30 seconds
    private final int THREAD_POOL_SIZE = 50;
    private final Map<Socket, Long> connectionTimestamps = new ConcurrentHashMap<>();
    private final long CONNECTION_TIMEOUT = 10000; // Reduced to 10 seconds
    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private final Set<Socket> activeSockets = Collections.synchronizedSet(new HashSet<>());
    private final Map<Socket, Thread> socketThreads = new ConcurrentHashMap<>();
    private final Map<Socket, Boolean> validConnections = new ConcurrentHashMap<>();
    private final Map<String, ReentrantReadWriteLock> fileLocks = new ConcurrentHashMap<>();
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
        Thread currentThread = Thread.currentThread();
        socketThreads.put(socket, currentThread);
        validConnections.put(socket, false); // Mark as invalid initially
        boolean connectionCounted = false;

        try {
            socket.setSoTimeout(SOCKET_TIMEOUT);
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(8192);
            socket.setSendBufferSize(8192);
            socket.setReuseAddress(true);
            socket.setSoLinger(true, 3);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            String action = in.readUTF();
            //  System.out.println("[NODE] Received action: " + action);

            // Only increment connection count for valid actions
            if (isValidAction(action)) {
                validConnections.put(socket, true);
                activeSockets.add(socket);
                connectionTimestamps.put(socket, System.currentTimeMillis());
                if (!connectionCounted) {
                    int currentLoad = activeConnections.incrementAndGet();
                    connectionCounted = true;
                    //  System.out.println("[NODE] New valid connection. Current load: " + currentLoad);
                }
            }

            // Update timestamp for active connection
            connectionTimestamps.put(socket, System.currentTimeMillis());

            if ("list".equals(action)) {
                String department = in.readUTF();
                handleListAction(out, department);
                return;
            }

            if ("ping".equals(action)) {
                try {
                    validConnections.put(socket, true);
                    activeSockets.add(socket);
                    connectionTimestamps.put(socket, System.currentTimeMillis());
                    if (!connectionCounted) {
                        int currentLoad = activeConnections.incrementAndGet();
                        connectionCounted = true;
                        System.out.println("[NODE] New valid connection. Current load: " + currentLoad);
                    }
                    // Keep the connection alive for repeated pings
                    while (true) {
                        out.writeUTF("pong");
                        out.flush();
                        connectionTimestamps.put(socket, System.currentTimeMillis());
                        // Wait for next ping
                        String nextAction = in.readUTF();
                        if (!"ping".equals(nextAction)) break;
                    }
                } catch (IOException e) {
                    //3  System.out.println("[NODE] Error sending pong response: " + e.getMessage());
                }
                return;
            }

            if ("getLoad".equals(action)) {
                try {
                    int load = activeConnections.get();
                    out.writeInt(load);
                    out.flush();
                    connectionTimestamps.put(socket, System.currentTimeMillis());
                } catch (IOException e) {
                    System.out.println("[NODE] Error sending load response: " + e.getMessage());
                }
                return;
            }

            String department = in.readUTF();
            String filename = in.readUTF();
            System.out.println("[NODE] Processing command: " + action + " for " + department + "/" + filename);

            // Track load for file operations
            if (isValidAction(action) && !connectionCounted) {
                int currentLoad = activeConnections.incrementAndGet();
                connectionCounted = true;
                System.out.println("[NODE] Processing file operation. Current load: " + currentLoad);
            }

            switch (action) {
                case "add", "edit" -> handleAddEditAction(in, out, department, filename);
                case "delete" -> handleDeleteAction(out, department, filename);
                case "fetch" -> handleFetchAction(out, department, filename);





                default -> {
                    System.out.println("[NODE] Invalid action: " + action);
                    out.writeBoolean(false);
                    out.flush();
                }
            }
        } catch (SocketTimeoutException e) {
            System.out.println("[NODE] Socket timeout - client may have disconnected");
        } catch (IOException e) {
            System.out.println("[NODE] IO Error: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("[NODE] Error handling client: " + e.getMessage());
        } finally {
            cleanupConnection(socket);
        }
    }
    private boolean isValidAction(String action) {
        return action != null && (
                action.equals("list") ||
                        action.equals("ping") ||
                        action.equals("getLoad") ||
                        action.equals("add") ||
                        action.equals("edit") ||
                        action.equals("delete") ||
                        action.equals("fetch")
        );
    }
    private void handleListAction(ObjectOutputStream out, String department) throws IOException {
        File deptDir = new File(baseDir, department);
        List<String> files = new ArrayList<>();

        System.out.println("[NODE] Listing files in directory: " + deptDir.getAbsolutePath());

        if (!deptDir.exists()) {
            boolean created = deptDir.mkdirs();
            System.out.println("[NODE] " + (created ? "Created" : "Failed to create") + " directory: " + deptDir.getAbsolutePath());
        }

        // For listing, we'll use read locks for each file to ensure consistency
        File[] listFiles = deptDir.listFiles();
        if (listFiles != null) {
            for (File f : listFiles) {
                if (f.isFile()) {
                    String fileKey = department + "/" + f.getName();
                    ReentrantReadWriteLock lock = fileLocks.computeIfAbsent(fileKey, k -> new ReentrantReadWriteLock());

                    // Acquire read lock for each file
                    lock.readLock().lock();
                    try {
                        files.add(f.getName());
                        System.out.println("[NODE] Found file: " + f.getName() + " (size: " + f.length() + " bytes)");
                    } finally {
                        lock.readLock().unlock();
                    }
                }
            }
        }

        System.out.println("[NODE] Sending " + files.size() + " files to client");
        out.writeObject(files);
        out.flush();
        System.out.println("[NODE] Successfully sent file list to client");
    }
    private void handleAddEditAction(ObjectInputStream in, ObjectOutputStream out, String department, String filename) throws Exception {
        String fileKey = department + "/" + filename;
        //a Write Lock for This File
        ReentrantReadWriteLock lock = fileLocks.computeIfAbsent(fileKey, k -> new ReentrantReadWriteLock());

        try {
            System.out.println("[NODE] Starting " + (new File(new File(baseDir, department), filename).exists() ? "edit" : "add") +
                    " operation for " + fileKey);

            // Acquire write lock for add/edit operations
            lock.writeLock().lock();
            try {
                byte[] content = (byte[]) in.readObject(); //Receive File Content From Client
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
            } finally {
                lock.writeLock().unlock();
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
        String fileKey = department + "/" + filename;
        ReentrantReadWriteLock lock = fileLocks.computeIfAbsent(fileKey, k -> new ReentrantReadWriteLock());

        // Acquire write lock for delete operation
        lock.writeLock().lock();
        try {
            File file = new File(new File(baseDir, department), filename);

            boolean deleted = file.delete();

            out.writeBoolean(deleted);
            fileLocks.remove(fileKey);
            System.out.println("[NODE] Delete " + filename + " result: " + deleted);
            out.flush();

        }

        catch (Exception e) {
            System.err.println("[NODE] Exception during delete: " + e.getMessage());
            e.printStackTrace();
            out.writeBoolean(false); // Avoid breaking the client
        }
        finally {
            lock.writeLock().unlock();
        }
    }
    private void handleFetchAction(ObjectOutputStream out, String department, String filename) throws IOException {
        String fileKey = department + "/" + filename;
        ReentrantReadWriteLock lock = fileLocks.computeIfAbsent(fileKey, k -> new ReentrantReadWriteLock());

        System.out.println("[NODE] Fetch request for: " + fileKey);
        File deptDir = new File(baseDir, department);
        File targetFile = new File(deptDir, filename);

        // Acquire read lock for fetch operation
        lock.readLock().lock();
        try {
            if (targetFile.exists() && targetFile.isFile()) {
                try {
                    byte[] data = Files.readAllBytes(targetFile.toPath());
                    System.out.println("[NODE] Sending " + data.length + " bytes");
                    out.writeObject(data);
                    out.flush();
                    System.out.println("[NODE] Successfully sent file data");
                } catch (IOException e) {
                    System.err.println("[NODE] Error reading file: " + e.getMessage());
                    out.writeObject(new byte[0]);
                    out.flush();
                }
            } else {
                System.out.println("[NODE] File not found");
                out.writeObject(new byte[0]);
                out.flush();
            }
        } finally {
            lock.readLock().unlock();
        }
    }
    public void start() throws IOException {
        startCleanupThread(); // Start the cleanup thread
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setSoTimeout(SOCKET_TIMEOUT);
            serverSocket.setReceiveBufferSize(65536);
            serverSocket.setReuseAddress(true);
            System.out.println("File Node running on port " + port);

            while (!Thread.currentThread().isInterrupted()) {
                if (!isOnline) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    continue;
                }

                try {
                    Socket socket = serverSocket.accept();
                    socket.setSoTimeout(SOCKET_TIMEOUT);
                    socket.setKeepAlive(true);
                    socket.setTcpNoDelay(true);
                    socket.setReceiveBufferSize(65536);
                    socket.setSendBufferSize(65536);
                    socket.setReuseAddress(true);
                    socket.setSoLinger(true, 5);


                    threadPool.execute(() -> handleClient(socket));
                } catch (SocketTimeoutException e) {
                    // Timeout is expected, continue looping
                } catch (IOException e) {
                    System.err.println("[NODE] Accept failed: " + e.getMessage());
                }
            }
        } finally {
            threadPool.shutdown();
            try {
                if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    threadPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                threadPool.shutdownNow();
            }
        }
    }
    private void cleanupConnection(Socket socket) {
        try {
            if (validConnections.getOrDefault(socket, false)) {
                activeSockets.remove(socket);
                if (activeConnections.get() > 0) {
                    int currentLoad = activeConnections.decrementAndGet();
                    //  System.out.println("[NODE] Connection cleaned up. Current load: " + currentLoad);
                }
            }
            connectionTimestamps.remove(socket);
            socketThreads.remove(socket);
            validConnections.remove(socket);

            if (!socket.isClosed()) {
                try {
                    socket.shutdownInput();
                    socket.shutdownOutput();
                } catch (IOException e) {
                    // Ignore shutdown errors
                }
                socket.close();
            }
        } catch (IOException e) {
            System.out.println("[NODE] Error cleaning up connection: " + e.getMessage());
        }
    }
    // Start connection cleanup thread with more frequent checks
    private void startCleanupThread() {
        Thread cleanupThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long currentTime = System.currentTimeMillis();
                    connectionTimestamps.entrySet().removeIf(entry -> {
                        if (currentTime - entry.getValue() > CONNECTION_TIMEOUT) {
                            Socket socket = entry.getKey();
                            cleanupConnection(socket);
                            return true;
                        }
                        return false;
                    });
                    Thread.sleep(5000); // Check every 5 seconds
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        cleanupThread.setDaemon(true);
        cleanupThread.start();
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