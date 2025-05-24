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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import server.interfaces.FileOperationService;

class FileNodeServer {
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
    private final FileOperationHandler fileOperationHandler;
    private final ConnectionManager connectionManager;
    private final LoadMonitor loadMonitor;

    // Define NodeInfo as a nested record
    public record NodeInfo(Thread thread, FileNodeServer server, Set<Thread> loadThreads) {}

    public FileNodeServer(int port, String baseDirPath) {
        this.port = port;
        this.baseDir = new File(baseDirPath);
        this.fileOperationHandler = new FileOperationHandler(baseDir);
        this.connectionManager = new ConnectionManager(activeConnections, activeSockets, connectionTimestamps,
                socketThreads, validConnections, CONNECTION_TIMEOUT);
        this.loadMonitor = new LoadMonitor(activeConnections, loadTestSockets);

        initializeDirectories();
    }

    private void initializeDirectories() {
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
        validConnections.put(socket, false);
        boolean connectionCounted = false;

        try {
            configureSocket(socket);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            String action = in.readUTF();

            if (isValidAction(action)) {
                connectionManager.registerValidConnection(socket);
                if (!connectionCounted) {
                    connectionCounted = true;
                    loadMonitor.incrementLoad();
                }
            }

            connectionTimestamps.put(socket, System.currentTimeMillis());

            if ("list".equals(action)) {
                String department = in.readUTF();
                fileOperationHandler.handleListAction(out, department);
                return;
            }

            if ("ping".equals(action)) {
                handlePingAction(in, out, socket, connectionCounted);
                return;
            }

            if ("getLoad".equals(action)) {
                handleGetLoadAction(out, socket);
                return;
            }

            String department = in.readUTF();
            String filename = in.readUTF();
            System.out.println("[NODE] Processing command: " + action + " for " + department + "/" + filename);

            if (isValidAction(action) && !connectionCounted) {
                connectionCounted = true;
                loadMonitor.incrementLoad();
            }

            switch (action) {
                case "add", "edit" -> fileOperationHandler.handleAddEditAction(in, out, department, filename);
                case "delete" -> fileOperationHandler.handleDeleteAction(out, department, filename);
                case "fetch" -> fileOperationHandler.handleFetchAction(out, department, filename);
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
            connectionManager.cleanupConnection(socket);
        }
    }

    private void configureSocket(Socket socket) throws IOException {
        socket.setSoTimeout(SOCKET_TIMEOUT);
        socket.setKeepAlive(true);
        socket.setTcpNoDelay(true);
        socket.setReceiveBufferSize(8192);
        socket.setSendBufferSize(8192);
        socket.setReuseAddress(true);
        socket.setSoLinger(true, 3);
    }

    private void handlePingAction(ObjectInputStream in, ObjectOutputStream out, Socket socket, boolean connectionCounted) {
        try {
            connectionManager.registerValidConnection(socket);
            if (!connectionCounted) {
                loadMonitor.incrementLoad();
            }
            while (true) {
                out.writeUTF("pong");
                out.flush();
                connectionTimestamps.put(socket, System.currentTimeMillis());
                String nextAction = in.readUTF();
                if (!"ping".equals(nextAction)) break;
            }
        } catch (IOException e) {
            // Ignore ping errors
        }
    }

    private void handleGetLoadAction(ObjectOutputStream out, Socket socket) {
        try {
            int load = loadMonitor.getCurrentLoad();
            out.writeInt(load);
            out.flush();
            connectionTimestamps.put(socket, System.currentTimeMillis());
        } catch (IOException e) {
            System.out.println("[NODE] Error sending load response: " + e.getMessage());
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

    public void start() throws IOException {
        connectionManager.startCleanupThread();
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            configureServerSocket(serverSocket);
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
                    configureSocket(socket);
                    threadPool.execute(() -> handleClient(socket));
                } catch (SocketTimeoutException e) {
                    // Timeout is expected, continue looping
                } catch (IOException e) {
                    System.err.println("[NODE] Accept failed: " + e.getMessage());
                }
            }
        } finally {
            shutdownThreadPool();
        }
    }

    private void configureServerSocket(ServerSocket serverSocket) throws IOException {
        serverSocket.setSoTimeout(SOCKET_TIMEOUT);
        serverSocket.setReceiveBufferSize(65536);
        serverSocket.setReuseAddress(true);
    }

    private void shutdownThreadPool() {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
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

        // Notify coordinator about node status change
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);
            FileOperationService fileService = (FileOperationService) registry.lookup("FileOperationService");
            fileService.updateNodeStatus(num - 1, true);
        } catch (Exception e) {
            System.out.println("Error notifying coordinator: " + e.getMessage());
        }
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

            // Notify coordinator about node status change
            try {
                Registry registry = LocateRegistry.getRegistry("localhost", 1099);
                FileOperationService fileService = (FileOperationService) registry.lookup("FileOperationService");
                fileService.updateNodeStatus(num - 1, false);
            } catch (Exception e) {
                System.out.println("Error notifying coordinator: " + e.getMessage());
            }
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