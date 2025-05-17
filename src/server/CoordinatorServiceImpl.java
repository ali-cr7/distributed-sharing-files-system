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
    protected CoordinatorServiceImpl() throws RemoteException {
        super();
        initializeNodes();
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
        if (!hasPermission(token, action, department)) return false;

        int selectedNode = getLeastLoadedNode();
        if (selectedNode == -1) {
            System.err.println("[COORDINATOR] No active nodes available.");
            return false;
        }

        nodeConnections.compute(selectedNode, (k, v) -> v + 1); // track load

        String nodeHost = nodeAddresses.get(selectedNode);
        int port = nodePorts.get(selectedNode);

        try (Socket socket = new Socket(nodeHost, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeUTF(action);
            out.writeUTF(department);
            out.writeUTF(filename);
            out.writeObject(content);

            boolean success = in.readBoolean();

            if (success && action.equalsIgnoreCase("add")) {
                fileLocationMap.put(department + "/" + filename, nodeHost + ":" + port);
            }

            return success;

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            nodeConnections.computeIfPresent(selectedNode, (k, v) -> Math.max(0, v - 1));
        }
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
            return List.of("Permission denied");
        }
        long startTime = System.currentTimeMillis();
// ... make the request ...
        long responseTime = System.currentTimeMillis() - startTime;

        int nodeId = loadBalancer.selectNode();
        loadBalancer.requestCompleted(nodeId, responseTime);
        System.out.println("selected node " +  nodeId );
        if (nodeId == -1) return List.of("No available nodes");

        NodeInfo node = nodeInfoMap.get(nodeId);
        node.activeConnections.incrementAndGet();

        try (Socket socket = new Socket(node.host, node.port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeUTF("list");
            out.writeUTF(department);
            out.flush();

            return (List<String>) in.readObject();
        } catch (Exception e) {
            System.err.println("[COORDINATOR] Error listing files: " + e.getMessage());
            // Mark node as temporarily unavailable
            loadBalancer.nodeFailed(nodeId);
            return Collections.emptyList();
        } finally {
            node.activeConnections.decrementAndGet();
            loadBalancer.requestCompleted(nodeId,responseTime);
        }
    }


    private static class LoadBalancer {
        private final Map<Integer, NodeStats> nodeStats = new ConcurrentHashMap<>();
        private final List<Integer> availableNodes = new CopyOnWriteArrayList<>();
        private final double CONNECTION_WEIGHT = 0.7;
        private final double RESPONSE_TIME_WEIGHT = 0.3;

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
                stats.avgResponseTime = (stats.avgResponseTime * stats.completedRequests + responseTime) /
                        (stats.completedRequests + 1);
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

        public NodeInfo(String host, int port, AtomicInteger activeConnections, boolean isActive) {
            this.host = host;
            this.port = port;
            this.activeConnections = activeConnections;
            this.isActive = isActive;
        }
    }
}


