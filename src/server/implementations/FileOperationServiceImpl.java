package server.implementations;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import server.interfaces.FileOperationService;

public class FileOperationServiceImpl extends BaseCoordinatorService implements FileOperationService {
    private final int MAX_RETRIES = 3;
    private final int CONNECTION_TIMEOUT = 3000; // 3 seconds
    private final int SOCKET_TIMEOUT = 5000; // 5 seconds
    private final int MAX_FAILURES = 3; // Maximum number of failures before marking node as inactive

    public FileOperationServiceImpl() throws RemoteException {
        super();
        initializeNodes();
    }

    private void initializeNodes() {
        // Only initialize if no nodes exist
        if (!nodeInfoMap.isEmpty()) {
            return;
        }

        List<String> addresses = List.of("localhost", "localhost", "localhost");
        List<Integer> ports = List.of(5001, 5002, 5003);

        for (int i = 0; i < ports.size(); i++) {
            nodeInfoMap.put(i, new NodeInfo(
                    addresses.get(i),
                    ports.get(i),
                    new AtomicInteger(0),
                    true
            ));
        }
    }

    private void updateNodeStatusInternal(int nodeId, boolean isActive) {
        NodeInfo node = nodeInfoMap.get(nodeId);
        if (node != null) {
            node.isActive = isActive;
            if (!isActive) {
                node.currentLoad = 0;
                node.failureCount = 0;
            }
            System.out.println("[COORDINATOR] Node " + nodeId + " status updated to: " + (isActive ? "ACTIVE" : "INACTIVE"));
        }
    }

    private boolean isNodeAvailable(int nodeId) {
        NodeInfo node = nodeInfoMap.get(nodeId);
        return node != null && node.isActive;
    }

    @Override
    public boolean sendFileCommand(String token, String action, String filename, String department, byte[] content) throws RemoteException {
        if (!hasPermission(token, action, department)) {
            System.out.println("[COORDINATOR] Permission denied for " + action + " operation in " + department);
            return false;
        }

        System.out.println("[COORDINATOR] Attempting " + action + " operation for " + department + "/" + filename);
        int retries = 0;

        String key = department + "/" + filename;
        if (action.equals("edit")) {
            String lockHolder = fileEditLocks.get(key);
            if (lockHolder != null && !lockHolder.equals(token)) {
                System.out.println("[COORDINATOR] Edit denied: file is locked by another user.");
                return false;
            }
        }

        while (retries < MAX_RETRIES) {
            List<Integer> activeNodes = new ArrayList<>();
            Map<Integer, Integer> nodeLoads = new HashMap<>();
            for (int i = 0; i < nodeInfoMap.size(); i++) {
                if (isNodeAvailable(i)) {
                    NodeInfo node = nodeInfoMap.get(i);
                    activeNodes.add(i);
                    nodeLoads.put(i, node.currentLoad);
                }
            }

            if (activeNodes.isEmpty()) {
                System.err.println("[COORDINATOR] No active nodes available.");
                return false;
            }

            if (action.equalsIgnoreCase("add")) {
                return handleAddOperation(activeNodes, nodeLoads, department, filename, content);
            }

            int selectedNode = activeNodes.stream()
                    .min(Comparator.comparingInt(nodeLoads::get))
                    .orElse(-1);

            if (selectedNode == -1) {
                System.err.println("[COORDINATOR] Failed to select a node.");
                return false;
            }

            NodeInfo node = nodeInfoMap.get(selectedNode);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(node.host, node.port), CONNECTION_TIMEOUT);
                socket.setSoTimeout(SOCKET_TIMEOUT);

                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                out.writeUTF(action);
                out.writeUTF(department);
                out.writeUTF(filename);
                if (content != null) {
                    out.writeObject(content);
                }
                out.flush();

                boolean success = in.readBoolean();
                if (success) {
                    if (action.equalsIgnoreCase("edit") || action.equalsIgnoreCase("delete")) {
                        fileLocationMap.put(department + "/" + filename, node.host + ":" + node.port);
                    }
                    return true;
                }
            } catch (Exception e) {
                System.err.println("[COORDINATOR] Error with node " + selectedNode + ": " + e.getMessage());
                NodeInfo failedNode = nodeInfoMap.get(selectedNode);
                if (failedNode != null) {
                    failedNode.failureCount++;
                    if (failedNode.failureCount >= MAX_FAILURES) {
                        updateNodeStatusInternal(selectedNode, false);
                    }
                }
                retries++;
            }
        }
        return false;
    }

    private boolean handleAddOperation(List<Integer> activeNodes, Map<Integer, Integer> nodeLoads,
                                       String department, String filename, byte[] content) {
        activeNodes.sort(Comparator.comparingInt(nodeLoads::get));
        int replicationFactor = Math.min(2, activeNodes.size());
        List<String> locations = new ArrayList<>();
        boolean allSuccess = true;

        for (int i = 0; i < replicationFactor; i++) {
            int nodeId = activeNodes.get(i);
            NodeInfo node = nodeInfoMap.get(nodeId);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(node.host, node.port), CONNECTION_TIMEOUT);
                socket.setSoTimeout(SOCKET_TIMEOUT);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                out.writeUTF("add");
                out.writeUTF(department);
                out.writeUTF(filename);
                out.writeObject(content);
                out.flush();
                boolean success = in.readBoolean();
                if (success) {
                    locations.add(node.host + ":" + node.port);
                } else {
                    allSuccess = false;
                }
            } catch (Exception e) {
                System.err.println("[COORDINATOR] Error with node " + nodeId + ": " + e.getMessage());
                allSuccess = false;
            }
        }
        if (!locations.isEmpty()) {
            fileLocationMap.put(department + "/" + filename, String.join(",", locations));
        }
        return allSuccess;
    }

    @Override
    public byte[] requestFile(String token, String filename, String department) throws RemoteException {
        if (!hasPermission(token, "view", department)) {
            System.out.println("[COORDINATOR] Permission denied for " + department);
            return null;
        }

        String key = department + "/" + filename;
        String locationList = fileLocationMap.get(key);
        if (locationList != null) {
            String[] locations = locationList.split(",");
            List<Integer> candidateNodes = new ArrayList<>();
            Map<Integer, Integer> nodeLoads = new HashMap<>();

            for (String loc : locations) {
                for (int i = 0; i < nodeInfoMap.size(); i++) {
                    NodeInfo node = nodeInfoMap.get(i);
                    if (node != null && node.isActive && (node.host + ":" + node.port).equals(loc)) {
                        candidateNodes.add(i);
                        nodeLoads.put(i, node.currentLoad);
                    }
                }
            }

            if (!candidateNodes.isEmpty()) {
                candidateNodes.sort(Comparator.comparingInt(nodeLoads::get));
                for (int nodeId : candidateNodes) {
                    NodeInfo node = nodeInfoMap.get(nodeId);
                    try (Socket socket = new Socket()) {
                        socket.connect(new InetSocketAddress(node.host, node.port), CONNECTION_TIMEOUT);
                        socket.setSoTimeout(SOCKET_TIMEOUT);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.flush();
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                        out.writeUTF("fetch");
                        out.writeUTF(department);
                        out.writeUTF(filename);
                        out.flush();
                        byte[] data = (byte[]) in.readObject();
                        if (data != null && data.length > 0) {
                            return data;
                        }
                    } catch (Exception e) {
                        System.err.println("[COORDINATOR] Node " + nodeId + " error: " + e.getMessage());
                    }
                }
            }
        }
        return new byte[0];
    }

    @Override
    public List<String> listFiles(String token, String department) throws RemoteException {
        if (!hasPermission(token, "view", department)) {
            System.out.println("[COORDINATOR] Permission denied for user to view department: " + department);
            return List.of("Permission denied");
        }

        List<String> result = new ArrayList<>();
        List<Integer> activeNodes = new ArrayList<>();

        for (int i = 0; i < nodeInfoMap.size(); i++) {
            NodeInfo node = nodeInfoMap.get(i);
            if (node != null && node.isActive) {
                activeNodes.add(i);
            }
        }

        if (activeNodes.isEmpty()) {
            return List.of("No available nodes");
        }

        for (int nodeId : activeNodes) {
            NodeInfo node = nodeInfoMap.get(nodeId);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(node.host, node.port), CONNECTION_TIMEOUT);
                socket.setSoTimeout(SOCKET_TIMEOUT);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                out.writeUTF("list");
                out.writeUTF(department);
                out.flush();
                @SuppressWarnings("unchecked")
                List<String> files = (List<String>) in.readObject();
                if (files != null) {
                    result.addAll(files);
                    if (!result.isEmpty()) {
                        return result;
                    }
                }
            } catch (Exception e) {
                System.err.println("[COORDINATOR] Error connecting to node " + nodeId + ": " + e.getMessage());
            }
        }
        return result.isEmpty() ? List.of("No files found") : result;
    }

    private boolean hasPermission(String token, String action, String department) throws RemoteException {
        String username = activeTokens.get(token);
        System.out.println("[DEBUG] Checking permission for token: " + token + ", username: " + username);
        if (username == null) {
            System.out.println("[DEBUG] No username found for token");
            return false;
        }
        User user = users.get(username);
        System.out.println("[DEBUG] User found: " + (user != null) + ", role: " + (user != null ? user.role : "null") + ", department: " + (user != null ? user.department : "null"));
        if (user == null) return false;
        if (user.role.equals("manager")) {
            System.out.println("[DEBUG] User is manager, granting access");
            return true;
        }
        boolean hasAccess = user.department.equals(department) && List.of("add", "edit", "delete", "view").contains(action);
        System.out.println("[DEBUG] Regular user access check: " + hasAccess);
        return hasAccess;
    }

    @Override
    public void updateNodeStatus(int nodeId, boolean isActive) throws RemoteException {
        updateNodeStatusInternal(nodeId, isActive);
    }
} 