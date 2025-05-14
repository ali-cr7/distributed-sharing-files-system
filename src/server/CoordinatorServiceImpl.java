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

class CoordinatorServiceImpl extends UnicastRemoteObject implements CoordinatorService {
    static class User {
        String username, password, role, department;
        User(String u, String p, String r, String d) {
            username = u; password = p; role = r; department = d;
        }
    }

    private final Map<String, User> users = new ConcurrentHashMap<>();
    private final Map<String, String> activeTokens = new ConcurrentHashMap<>();
    private final List<String> nodeAddresses = List.of("localhost", "localhost", "localhost");
    private final List<Integer> nodePorts = List.of(5001, 5002, 5003);
    private int nodeIndex = 0;

    private final Map<String, String> fileLocationMap = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> nodeLoad = new ConcurrentHashMap<>();
    private final Map<Integer, Boolean> nodeStatus = new ConcurrentHashMap<>();

    protected CoordinatorServiceImpl() throws RemoteException {
        super();
        for (int i = 0; i < nodePorts.size(); i++) {
            nodeLoad.put(i, 0);
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

        int selectedNode = selectBestNode();
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
            nodeLoad.computeIfPresent(selectedNode, (k, v) -> Math.max(0, v - 1));
        }
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

        // fallback search across all nodes
        for (int i = 0; i < nodeAddresses.size(); i++) {
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
                    fileLocationMap.put(key, host + ":" + port); // cache for future
                    return data;
                }
            } catch (Exception e) {
                System.out.println("[COORDINATOR] Node " + i + " error: " + e.getMessage());
            }
        }
        return new byte[0];
    }

    @Override
    public List<String> listFiles(String token, String department) throws RemoteException {
        Set<String> uniqueFiles = new HashSet<>();

        for (int i = 0; i < nodeAddresses.size(); i++) {
            try (Socket socket = new Socket(nodeAddresses.get(i), nodePorts.get(i))) {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();

                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                out.writeUTF("list");
                out.writeUTF(department);
                out.flush();

                List<String> nodeFiles = (List<String>) in.readObject();
                uniqueFiles.addAll(nodeFiles);
            } catch (Exception e) {
                System.err.println("Error querying node " + (i + 1) + ": " + e.getMessage());
            }
        }
        return new ArrayList<>(uniqueFiles);
    }

    private synchronized int selectBestNode() {
        int attempts = nodeAddresses.size();
        int startIndex = nodeIndex;
        int bestIndex = startIndex;
        int minLoad = Integer.MAX_VALUE;

        for (int i = 0; i < attempts; i++) {
            int currentIndex = (startIndex + i) % nodeAddresses.size();
            int load = nodeLoad.getOrDefault(currentIndex, 0);
            if (load < minLoad) {
                minLoad = load;
                bestIndex = currentIndex;
            }
        }

        nodeLoad.computeIfPresent(bestIndex, (k, v) -> v + 1);
        nodeIndex = (bestIndex + 1) % nodeAddresses.size();

        System.out.println("[COORDINATOR] Selected node " + bestIndex + " (Load: " + minLoad + ")");
        return bestIndex;
    }
    @Override
    public void simulateLoadOnNode(int nodeIndex, int loadAmount) throws RemoteException {
        if (nodeIndex < 0 || nodeIndex >= nodeLoad.size()) {
            throw new RemoteException("Invalid node index");
        }

        // Add the load amount to the node's current load
        nodeLoad.computeIfPresent(nodeIndex, (k, v) -> v + loadAmount);

        System.out.println("[COORDINATOR] Simulated load of " + loadAmount + " on node " + nodeIndex + ". Current load: " + nodeLoad.get(nodeIndex));
    }

    @Override
    public void setNodeStatus(int nodeIndex, boolean isActive) throws RemoteException {
        if (nodeIndex < 0 || nodeIndex >= nodeAddresses.size()) {
            throw new RemoteException("Invalid node index");
        }

        nodeStatus.put(nodeIndex, isActive);
        String status = isActive ? "active" : "inactive";
        System.out.println("[COORDINATOR] Node " + nodeIndex + " is now " + status);
    }

}
