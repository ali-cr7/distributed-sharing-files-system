package server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

class CoordinatorServiceImpl extends UnicastRemoteObject implements CoordinatorService {
    static class User {
        String username, password, role, department;
        User(String u, String p, String r, String d) {
            username = u; password = p; role = r; department = d;
        }
    }

    private final Map<String, User> users = new ConcurrentHashMap<>();
    private final Map<String, String> activeTokens = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> nodeConnections = new ConcurrentHashMap<>();
    private final List<String> nodeAddresses = List.of("localhost", "localhost", "localhost");
    private final List<Integer> nodePorts = List.of(5001, 5002, 5003);

    private final Map<String, String> fileLocationMap = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> nodeLoad = new ConcurrentHashMap<>();
    private final Map<Integer, Boolean> nodeStatus = new ConcurrentHashMap<>();
    private final Map<Integer, NodeInfo> nodeInfoMap = new ConcurrentHashMap<>();
    private final LoadBalancer loadBalancer = new LoadBalancer();
    private Timer loadUpdateTimer;
    private Timer healthCheckTimer;
    private final int MAX_RETRIES = 3;
    private final int HEALTH_CHECK_INTERVAL = 5000; // 5 seconds

    protected CoordinatorServiceImpl() throws RemoteException {
        super();
        initializeNodes();
        startLoadUpdates();
        startHealthChecks();
    }

    private void initializeNodes() {
        List<String> addresses = List.of("localhost", "localhost", "localhost");
        List<Integer> ports = List.of(5001, 5002, 5003);

        for (int i = 0; i < ports.size(); i++) {
            nodeInfoMap.put(i, new NodeInfo(
                    addresses.get(i),
                    ports.get(i),
                    new AtomicInteger(0),
                    true
            ));
            loadBalancer.addNode(i);
        }
    }

    private void startLoadUpdates() {
        loadUpdateTimer = new Timer(true);
        loadUpdateTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateNodeLoads();
            }
        }, 0, 2000); // Update every 2 seconds
    }

    private void startHealthChecks() {
        healthCheckTimer = new Timer(true);
        healthCheckTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                checkNodeHealth();
            }
        }, 0, HEALTH_CHECK_INTERVAL);
    }

    private void updateNodeLoads() {
        for (int i = 0; i < nodeInfoMap.size(); i++) {
            NodeInfo node = nodeInfoMap.get(i);
            if (node != null && node.isActive) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(node.host, node.port), 1000);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    
                    out.writeUTF("getLoad");
                    out.flush();
                    
                    int load = in.readInt();
                    if (load != node.currentLoad) {
                        node.currentLoad = load;
                        System.out.println("[COORDINATOR] Node " + i + " load: " + load + " connections");
                    }
                } catch (IOException e) {
                    System.err.println("[COORDINATOR] Failed to update load for node " + i + ": " + e.getMessage());
                }
            }
        }
    }

    private void checkNodeHealth() {
        for (int i = 0; i < nodeInfoMap.size(); i++) {
            NodeInfo node = nodeInfoMap.get(i);
            if (node != null) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(node.host, node.port), 1000);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    
                    out.writeUTF("ping");
                    out.flush();
                    
                    String response = in.readUTF();
                    if ("pong".equals(response)) {
                        if (!node.isActive) {
                            System.out.println("[COORDINATOR] Node " + i + " is back online");
                            node.isActive = true;
                            node.failureCount = 0;
                        }
                    }
                } catch (IOException e) {
                    if (node.isActive) {
                        node.failureCount++;
                        System.err.println("[COORDINATOR] Node " + i + " health check failed: " + e.getMessage());
                        if (node.failureCount >= 3) {
                            System.err.println("[COORDINATOR] Node " + i + " marked as offline after " + node.failureCount + " failures");
                            node.isActive = false;
                            // Redistribute files from failed node
                            redistributeFilesFromNode(i);
                        }
                    }
                }
            }
        }
    }

    private void redistributeFilesFromNode(int failedNodeId) {
        NodeInfo failedNode = nodeInfoMap.get(failedNodeId);
        if (failedNode == null) return;

        String failedNodeAddress = failedNode.host + ":" + failedNode.port;
        List<String> filesToRedistribute = new ArrayList<>();

        // Find all files stored on the failed node
        for (Map.Entry<String, String> entry : fileLocationMap.entrySet()) {
            if (entry.getValue().equals(failedNodeAddress)) {
                filesToRedistribute.add(entry.getKey());
            }
        }

        System.out.println("[COORDINATOR] Attempting to recover " + filesToRedistribute.size() + " files from failed node " + failedNodeId);

        // First try to recover files from the failed node before it goes offline
        for (String fileKey : filesToRedistribute) {
            String[] parts = fileKey.split("/");
            String department = parts[0];
            String filename = parts[1];

            // Try to get file from failed node first
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(failedNode.host, failedNode.port), 1000);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                out.writeUTF("fetch");
                out.writeUTF(department);
                out.writeUTF(filename);
                out.flush();

                byte[] data = (byte[]) in.readObject();
                if (data != null && data.length > 0) {
                    // Find a new node to store the file
                    for (int i = 0; i < nodeInfoMap.size(); i++) {
                        if (i != failedNodeId) {
                            NodeInfo node = nodeInfoMap.get(i);
                            if (node != null && node.isActive) {
                                try (Socket newSocket = new Socket()) {
                                    newSocket.connect(new InetSocketAddress(node.host, node.port), 1000);
                                    ObjectOutputStream newOut = new ObjectOutputStream(newSocket.getOutputStream());
                                    ObjectInputStream newIn = new ObjectInputStream(newSocket.getInputStream());

                                    newOut.writeUTF("add");
                                    newOut.writeUTF(department);
                                    newOut.writeUTF(filename);
                                    newOut.writeObject(data);
                                    newOut.flush();

                                    boolean success = newIn.readBoolean();
                                    if (success) {
                                        fileLocationMap.put(fileKey, node.host + ":" + node.port);
                                        System.out.println("[COORDINATOR] Successfully recovered and redistributed file " + filename + " to node " + i);
                                        break;
                                    }
                                } catch (Exception e) {
                                    System.err.println("[COORDINATOR] Failed to redistribute file " + filename + " to node " + i);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("[COORDINATOR] Failed to recover file " + filename + " from failed node " + failedNodeId);
                
                // If failed to get from failed node, try other nodes as backup
                for (int i = 0; i < nodeInfoMap.size(); i++) {
                    if (i != failedNodeId) {
                        NodeInfo node = nodeInfoMap.get(i);
                        if (node != null && node.isActive) {
                            try (Socket socket = new Socket()) {
                                socket.connect(new InetSocketAddress(node.host, node.port), 1000);
                                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                                out.writeUTF("fetch");
                                out.writeUTF(department);
                                out.writeUTF(filename);
                                out.flush();

                                byte[] data = (byte[]) in.readObject();
                                if (data != null && data.length > 0) {
                                    fileLocationMap.put(fileKey, node.host + ":" + node.port);
                                    System.out.println("[COORDINATOR] Recovered file " + filename + " from backup node " + i);
                                    break;
                                }
                            } catch (Exception ex) {
                                System.err.println("[COORDINATOR] Failed to recover file " + filename + " from backup node " + i);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean registerUser(String token, String username, String password, String role, String department) throws RemoteException {
        if (users.isEmpty() && username.equals("admin") && role.equals("manager")) {
            users.put(username, new User(username, password, role, department));
            return true;
        }

        List<String> allowedDepartments = List.of("QA", "Graphic", "Development", "general");
        if (!allowedDepartments.contains(department)) return false;

        String requester = activeTokens.get(token);
        if (requester == null) return false;

        User currentUser = users.get(requester);
        if (currentUser == null || !currentUser.role.equals("manager")) return false;
        if (users.containsKey(username)) return false;

        users.put(username, new User(username, password, role, department));
        return true;
    }

    @Override
    public String login(String username, String password) throws RemoteException {
        User user = users.get(username);
        if (user == null || !user.password.equals(password)) return null;
        String token = UUID.randomUUID().toString();
        activeTokens.put(token, username);
        return token;
    }

    @Override
    public boolean hasPermission(String token, String action, String department) throws RemoteException {
        String username = activeTokens.get(token);
        if (username == null) return false;
        User user = users.get(username);
        if (user == null) return false;
        if (user.role.equals("manager")) return true;
        return user.department.equals(department) && List.of("add", "edit", "delete", "view").contains(action);
    }

    @Override
    public boolean sendFileCommand(String token, String action, String filename, String department, byte[] content) throws RemoteException {
        if (!hasPermission(token, action, department)) {
            System.out.println("[COORDINATOR] Permission denied for " + action + " operation in " + department);
            return false;
        }

        System.out.println("[COORDINATOR] Attempting " + action + " operation for " + department + "/" + filename);
        int retries = 0;

        while (retries < MAX_RETRIES) {
            // Get active nodes and their current loads
            List<Integer> activeNodes = new ArrayList<>();
            Map<Integer, Integer> nodeLoads = new HashMap<>();
            
            for (int i = 0; i < nodeInfoMap.size(); i++) {
                NodeInfo node = nodeInfoMap.get(i);
                if (node != null && node.isActive) {
                    activeNodes.add(i);
                    nodeLoads.put(i, node.currentLoad);
                }
            }

            if (activeNodes.isEmpty()) {
                System.err.println("[COORDINATOR] No active nodes available.");
                return false;
            }

            // Select the node with the lowest load
            int selectedNode = activeNodes.stream()
                .min(Comparator.comparingInt(nodeLoads::get))
                .orElse(-1);

            if (selectedNode == -1) {
                System.err.println("[COORDINATOR] Failed to select a node.");
                return false;
            }

            NodeInfo node = nodeInfoMap.get(selectedNode);
            System.out.println("\n[COORDINATOR] Selected node " + selectedNode + " (Load: " + node.currentLoad + " connections)");

            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(node.host, node.port), 3000);
                socket.setSoTimeout(3000);

                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                System.out.println("[COORDINATOR] Connected to node " + selectedNode + " for " + action + " operation");

                out.writeUTF(action);
                out.writeUTF(department);
                out.writeUTF(filename);
                if (content != null) {
                    out.writeObject(content);
                }
                out.flush();

                System.out.println("[COORDINATOR] Sent command to node " + selectedNode + ", waiting for response...");
                boolean success = in.readBoolean();

                if (success) {
                    if (action.equalsIgnoreCase("add")) {
                        fileLocationMap.put(department + "/" + filename, node.host + ":" + node.port);
                        System.out.println("[COORDINATOR] File operation completed successfully on node " + selectedNode);
                    } else {
                        System.out.println("[COORDINATOR] " + action + " operation completed successfully on node " + selectedNode);
                    }
                    return true;
                } else {
                    System.err.println("[COORDINATOR] Node " + selectedNode + " reported operation failure");
                }
            } catch (IOException e) {
                System.err.println("[COORDINATOR] Error with node " + selectedNode + ": " + e.getMessage());
                node.failureCount++;
                if (node.failureCount >= 3) {
                    System.err.println("[COORDINATOR] Node " + selectedNode + " marked as offline after " + node.failureCount + " failures");
                    node.isActive = false;
                    redistributeFilesFromNode(selectedNode);
                }
                retries++;
            }
        }

        System.err.println("[COORDINATOR] Failed to execute " + action + " operation after " + MAX_RETRIES + " retries");
        return false;
    }

    @Override
    public byte[] requestFile(String token, String filename, String department) throws RemoteException {
        if (!hasPermission(token, "view", department)) {
            System.out.println("[COORDINATOR] Permission denied for " + department);
            return null;
        }

        String key = department + "/" + filename;
        String location = fileLocationMap.get(key);

        if (location != null) {
            try {
                String[] parts = location.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);

                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(host, port), 3000);
                    socket.setSoTimeout(3000);

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                    out.writeUTF("fetch");
                    out.writeUTF(department);
                    out.writeUTF(filename);
                    out.flush();

                    return (byte[]) in.readObject();
                }
            } catch (Exception e) {
                System.err.println("[COORDINATOR] Failed to contact node for file: " + e.getMessage());
            }
        }

        for (int i = 0; i < nodeAddresses.size(); i++) {
            if (!nodeStatus.getOrDefault(i, true)) continue;

            String host = nodeAddresses.get(i);
            int port = nodePorts.get(i);

            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 3000);
                socket.setSoTimeout(3000);

                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();

                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                out.writeUTF("fetch");
                out.writeUTF(department);
                out.writeUTF(filename);
                out.flush();

                byte[] data = (byte[]) in.readObject();
                if (data != null && data.length > 0) {
                    fileLocationMap.put(key, host + ":" + port); // cache it
                    return data;
                }
            } catch (Exception e) {
                System.out.println("[COORDINATOR] Node " + i + " error: " + e.getMessage());
            }
        }
        return new byte[0];
    }

    private int getLeastLoadedNode() {
        return nodeConnections.entrySet().stream()
                .filter(entry -> nodeStatus.getOrDefault(entry.getKey(), true))
                .min(Comparator.comparingInt(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .orElse(-1);
    }

    @Override
    public List<String> listUsers(String token) throws RemoteException {
        String username = activeTokens.get(token);
        if (username == null) return List.of("Access Denied: Invalid token");
        User user = users.get(username);
        if (!"manager".equals(user.role)) return List.of("Access Denied: Not a manager");

        List<String> result = new ArrayList<>();
        for (User u : users.values()) {
            result.add("Username: " + u.username + ", Role: " + u.role + ", Department: " + u.department);
        }
        return result;
    }

    @Override
    public List<String> listFiles(String token, String department) throws RemoteException {
        if (!hasPermission(token, "view", department)) {
            System.out.println("[COORDINATOR] Permission denied for user to view department: " + department);
            return List.of("Permission denied");
        }

        System.out.println("[COORDINATOR] Attempting to list files in " + department);
        List<String> result = new ArrayList<>();

        // Get a random active node
        List<Integer> activeNodes = new ArrayList<>();
        for (int i = 0; i < nodeInfoMap.size(); i++) {
            NodeInfo node = nodeInfoMap.get(i);
            if (node != null && node.isActive) {
                activeNodes.add(i);
            }
        }

        if (activeNodes.isEmpty()) {
            System.out.println("[COORDINATOR] No available nodes for listing files");
            return List.of("No available nodes");
        }

        int selectedNode = activeNodes.get(new Random().nextInt(activeNodes.size()));
        System.out.println("[COORDINATOR] Selected node " + selectedNode + " for listing files");
        NodeInfo node = nodeInfoMap.get(selectedNode);

        try (Socket socket = new Socket(node.host, node.port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            System.out.println("[COORDINATOR] Connected to node " + selectedNode + " for listing files");
            
            out.writeUTF("list");
            out.writeUTF(department);
            out.flush();

            @SuppressWarnings("unchecked")
            List<String> files = (List<String>) in.readObject();
            if (files != null) {
                result.addAll(files);
                System.out.println("[COORDINATOR] Retrieved " + files.size() + " files from node " + selectedNode);
            } else {
                System.out.println("[COORDINATOR] Received null file list from node " + selectedNode);
            }
        } catch (Exception e) {
            System.err.println("[COORDINATOR] Error listing files from node " + selectedNode + ": " + e.getMessage());
            e.printStackTrace();
            return List.of("Error retrieving file list: " + e.getMessage());
        }

        return result;
    }

    private static class LoadBalancer {
        private final Map<Integer, NodeStats> nodeStats = new ConcurrentHashMap<>();
        private final List<Integer> availableNodes = new CopyOnWriteArrayList<>();
        private final double CONNECTION_WEIGHT = 0.8; // Increased weight for connections
        private final double RESPONSE_TIME_WEIGHT = 0.2; // Decreased weight for response time

        public void addNode(int nodeId) {
            nodeStats.putIfAbsent(nodeId, new NodeStats());
            if (!availableNodes.contains(nodeId)) {
                availableNodes.add(nodeId);
            }
        }

        public int selectNode() {
            if (availableNodes.isEmpty()) {
                System.out.println("[LoadBalancer] No available nodes!");
                return -1;
            }

            int selectedNode = availableNodes.stream()
                    .min(Comparator.comparingDouble(nodeId -> {
                        NodeStats stats = nodeStats.get(nodeId);
                        double score = (stats.activeConnections * CONNECTION_WEIGHT) +
                                (stats.avgResponseTime * RESPONSE_TIME_WEIGHT);
                        System.out.printf("[LoadBalancer] Node %d - Connections: %d, Avg Response: %.2fms, Score: %.2f%n",
                                nodeId, stats.activeConnections, stats.avgResponseTime, score);
                        return score;
                    }))
                    .orElse(-1);

            System.out.printf("[LoadBalancer] Selected node %d for request%n", selectedNode);
            return selectedNode;
        }

        public void requestCompleted(int nodeId, long responseTime) {
            NodeStats stats = nodeStats.get(nodeId);
            if (stats != null) {
                stats.completedRequests++;
                // Update moving average of response time
                stats.avgResponseTime = (stats.avgResponseTime * (stats.completedRequests - 1) + responseTime) /
                        stats.completedRequests;
            }
        }

        public void nodeFailed(int nodeId) {
            availableNodes.removeIf(id -> id == nodeId);
            // Schedule re-check after delay
            new Timer().schedule(new TimerTask() {
                @Override
                public void run() {
                    addNode(nodeId);  // Will add back if not already present
                }
            }, 5000); // Retry after 5 seconds
        }

        private static class NodeStats {
            int activeConnections;
            double avgResponseTime;
            int completedRequests;

            public NodeStats() {
                this.activeConnections = 0;
                this.avgResponseTime = 0;
                this.completedRequests = 0;
            }
        }
    }

    private static class NodeStats {
        int completedRequests;
        double avgResponseTime;
        int activeConnections;
    }

    private static class NodeInfo {
        String host;
        int port;
        AtomicInteger activeConnections;
        boolean isActive;
        int currentLoad;
        int failureCount;

        public NodeInfo(String host, int port, AtomicInteger activeConnections, boolean isActive) {
            this.host = host;
            this.port = port;
            this.activeConnections = activeConnections;
            this.isActive = isActive;
            this.currentLoad = 0;
            this.failureCount = 0;
        }
    }
}


