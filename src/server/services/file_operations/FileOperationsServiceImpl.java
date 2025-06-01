package server.services.file_operations;
import server.services.auth.AuthServices;
import server.utility.Config;
import server.utility.LoadBalancer;
import server.utility.NodeInfo;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import static server.utility.Config.HEALTH_CHECK_INTERVAL;
import static server.utility.Config.LOAD_UPDATE_INTERVAL;
public class FileOperationsServiceImpl extends UnicastRemoteObject implements FileOperationsService {
    private final Map<String, String> fileLocationMap = new ConcurrentHashMap<>();
    private final Map<Integer, NodeInfo> nodeInfoMap = new ConcurrentHashMap<>();
    private final LoadBalancer loadBalancer = new LoadBalancer();
    private Timer loadUpdateTimer;
    private Timer healthCheckTimer;
    private final Map<Integer, Long> lastSuccessfulHealthCheck = new ConcurrentHashMap<>();
    private final Map<Integer, Long> lastSuccessfulLoadUpdate = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> consecutiveFailures = new ConcurrentHashMap<>();
    private final Map<Integer, Boolean> nodeRecoveryInProgress = new ConcurrentHashMap<>();
    private final Map<String, String> fileEditLocks = new ConcurrentHashMap<>();
    private final AuthServices authService;
    public FileOperationsServiceImpl(AuthServices authService) throws RemoteException {
        super();
        this.authService = authService;
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
                try {
                    updateNodeLoads();
                } catch (Exception e) {
                    System.err.println("[COORDINATOR] Error updating node loads: " + e.getMessage());
                }
            }
        }, 5000, LOAD_UPDATE_INTERVAL); // Start after 5 seconds, then update every LOAD_UPDATE_INTERVAL
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
                    socket.connect(new InetSocketAddress(node.host, node.port), Config.CONNECTION_TIMEOUT);
                    socket.setSoTimeout(Config.SOCKET_TIMEOUT);
                    socket.setKeepAlive(true);
                    socket.setTcpNoDelay(true);
                    socket.setReceiveBufferSize(8192);
                    socket.setSendBufferSize(8192);

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                    out.writeUTF("getLoad");
                    out.flush();

                    int load = in.readInt();
                    if (load != node.currentLoad) {
                        node.currentLoad = load;
                        lastSuccessfulLoadUpdate.put(i, System.currentTimeMillis());
                        System.out.println("[COORDINATOR] Node " + i + " load: " + load + " connections");
                    }
                } catch (IOException e) {
                    System.err.println("[COORDINATOR] Failed to update load for node " + i + ": " + e.getMessage());
                    // Reset load to 0 if we can't get the load
                    if (node.currentLoad != 0) {
                        node.currentLoad = 0;
                        System.out.println("[COORDINATOR] Reset node " + i + " load to 0 due to connection failure");
                    }
                }
            } else if (node != null && !node.isActive && node.currentLoad != 0) {
                // Reset load for inactive nodes
                node.currentLoad = 0;
                System.out.println("[COORDINATOR] Reset inactive node " + i + " load to 0");
            }
        }
    }
    private void checkNodeHealth() {
        for (int i = 0; i < nodeInfoMap.size(); i++) {
            final int nodeId = i;
            NodeInfo node = nodeInfoMap.get(nodeId);
            if (node != null) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(node.host, node.port), Config.CONNECTION_TIMEOUT);
                    socket.setSoTimeout(Config.SOCKET_TIMEOUT);
                    socket.setKeepAlive(true);
                    socket.setTcpNoDelay(true);
                    socket.setReceiveBufferSize(8192);
                    socket.setSendBufferSize(8192);

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                    out.writeUTF("ping");
                    out.flush();

                    String response = in.readUTF();
                    if ("pong".equals(response)) {
                        if (!node.isActive) {
                            System.out.println("[COORDINATOR] Node " + nodeId + " is back online");
                            node.isActive = true;
                            node.failureCount = 0;
                            consecutiveFailures.put(nodeId, 0);
                            nodeRecoveryInProgress.remove(nodeId);
                        }
                        lastSuccessfulHealthCheck.put(nodeId, System.currentTimeMillis());
                    }
                } catch (IOException e) {
                    if (node.isActive) {
                        int failures = consecutiveFailures.getOrDefault(nodeId, 0) + 1;
                        consecutiveFailures.put(nodeId, failures);
                        System.err.println("[COORDINATOR] Node " + nodeId + " health check failed: " + e.getMessage() +
                                " (Consecutive failures: " + failures + ")");

                        if (failures >= Config.MAX_FAILURES) {
                            System.err.println("[COORDINATOR] Node " + nodeId + " marked as offline after " + failures + " consecutive failures");
                            node.isActive = false;
                            node.currentLoad = 0; // Reset load when node goes offline
                            if (!nodeRecoveryInProgress.getOrDefault(nodeId, false)) {
                                nodeRecoveryInProgress.put(nodeId, true);
                                new Thread(() -> redistributeFilesFromNode(nodeId)).start();
                            }
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

        // Find all files stored on the failed node (may be in a list of locations)
        for (Map.Entry<String, String> entry : fileLocationMap.entrySet()) {
            String[] locations = entry.getValue().split(",");
            for (String loc : locations) {
                if (loc.equals(failedNodeAddress)) {
                    filesToRedistribute.add(entry.getKey());
                    break;
                }
            }
        }

        System.out.println("[COORDINATOR] Attempting to recover " + filesToRedistribute.size() + " files from failed node " + failedNodeId);

        for (String fileKey : filesToRedistribute) {
            String[] parts = fileKey.split("/");
            String department = parts[0];
            String filename = parts[1];
            byte[] data = null;
            // Try to get file from any other node that has it
            String[] locations = fileLocationMap.get(fileKey).split(",");
            for (String loc : locations) {
                if (!loc.equals(failedNodeAddress)) {
                    String[] hostPort = loc.split(":");
                    String host = hostPort[0];
                    int port = Integer.parseInt(hostPort[1]);
                    try (Socket socket = new Socket()) {
                        socket.connect(new InetSocketAddress(host, port), Config.CONNECTION_TIMEOUT);
                        socket.setSoTimeout(Config.SOCKET_TIMEOUT);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                        out.writeUTF("fetch");
                        out.writeUTF(department);
                        out.writeUTF(filename);
                        out.flush();
                        data = (byte[]) in.readObject();
                        if (data != null && data.length > 0) {
                            System.out.println("[COORDINATOR] Recovered file " + filename + " from backup node " + host + ":" + port);
                            break;
                        }
                    } catch (Exception e) {
                        System.err.println("[COORDINATOR] Failed to recover file " + filename + " from backup node " + host + ":" + port);
                    }
                }
            }
            // If recovered, redistribute to another active node
            if (data != null && data.length > 0) {
                Set<String> updatedLocs = new HashSet<>(Arrays.asList(locations));
                updatedLocs.remove(failedNodeAddress); // Remove the failed one

                for (int i = 0; i < nodeInfoMap.size(); i++) {
                    if (i != failedNodeId) {
                        NodeInfo node = nodeInfoMap.get(i);
                        if (node != null && node.isActive) {
                            String newLoc = node.host + ":" + node.port;

                            // ✅ Avoid re-sending to a node that already has the file
                            if (updatedLocs.contains(newLoc)) continue;

                            try (Socket newSocket = new Socket()) {
                                newSocket.connect(new InetSocketAddress(node.host, node.port), Config.CONNECTION_TIMEOUT);
                                newSocket.setSoTimeout(Config.SOCKET_TIMEOUT);
                                ObjectOutputStream newOut = new ObjectOutputStream(newSocket.getOutputStream());
                                ObjectInputStream newIn = new ObjectInputStream(newSocket.getInputStream());
                                newOut.writeUTF("add");
                                newOut.writeUTF(department);
                                newOut.writeUTF(filename);
                                newOut.writeObject(data);
                                newOut.flush();
                                boolean success = newIn.readBoolean();
                                if (success) {
                                    updatedLocs.add(newLoc);
                                    System.out.println("[COORDINATOR] Successfully redistributed file " + filename + " to node " + i);
                                } else {
                                    System.err.println("[COORDINATOR] Node " + i + " failed to accept file " + filename);
                                }
                            } catch (Exception e) {
                                System.err.println("[COORDINATOR] Failed to redistribute file " + filename + " to node " + i);
                            }
                        }
                    }
                }

                // 🔄 Update fileLocationMap after all attempts
                fileLocationMap.put(fileKey, String.join(",", updatedLocs));
            }
            else {
                System.err.println("[COORDINATOR] Failed to recover file " + filename + " from any backup node");
            }
        }
        nodeRecoveryInProgress.remove(failedNodeId);
    }
    @Override
    public boolean sendFileCommand(String token, String action, String filename, String department, byte[] content) throws RemoteException {
        if (!authService.hasPermission(token, action, department)) {
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

        while (retries < Config.MAX_RETRIES) {
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

            // For add: replicate to 2 least-loaded nodes
            if (action.equalsIgnoreCase("add")) {
                // Sort nodes by load
                activeNodes.sort(Comparator.comparingInt(nodeLoads::get));
                int replicationFactor = Math.min(2, activeNodes.size());
                List<String> locations = new ArrayList<>();
                boolean allSuccess = true;
                for (int i = 0; i < replicationFactor; i++) {
                    int nodeId = activeNodes.get(i);
                    NodeInfo node = nodeInfoMap.get(nodeId);
                    try (Socket socket = new Socket()) {
                        socket.connect(new InetSocketAddress(node.host, node.port), 3000);
                        socket.setSoTimeout(3000);
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
                            System.out.println("[COORDINATOR] File replicated to node " + nodeId);
                        } else {
                            System.err.println("[COORDINATOR] Node " + nodeId + " reported operation failure");
                            allSuccess = false;
                        }
                    } catch (Exception e) {
                        System.err.println("[COORDINATOR] Error with node " + nodeId + ": " + e.getMessage());
                        node.failureCount++;
                        if (node.failureCount >= 3) {
                            System.err.println("[COORDINATOR] Node " + nodeId + " marked as offline after " + node.failureCount + " failures");
                            node.isActive = false;
                            redistributeFilesFromNode(nodeId);
                        }
                        allSuccess = false;
                    }
                }
                if (!locations.isEmpty()) {
                    fileLocationMap.put(department + "/" + filename, String.join(",", locations));
                }
                return allSuccess;
            }

            // For other actions, use the least-loaded node
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
                    if (action.equalsIgnoreCase("edit") || action.equalsIgnoreCase("delete")) {
                        fileLocationMap.put(department + "/" + filename, node.host + ":" + node.port);
                        System.out.println("[COORDINATOR] File operation completed successfully on node " + selectedNode);
                    } else {
                        System.out.println("[COORDINATOR] " + action + " operation completed successfully on node " + selectedNode);
                    }
                    return true;
                } else {
                    System.err.println("[COORDINATOR] Node " + selectedNode + " reported operation failure");
                }
            } catch (Exception e) {
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

        System.err.println("[COORDINATOR] Failed to execute " + action + " operation after " + Config.MAX_RETRIES + " retries");
        return false;
    }
    @Override
    public byte[] requestFile(String token, String filename, String department) throws RemoteException {
//        if (!authService.hasPermission(token, "view", department)) {
//            System.out.println("[COORDINATOR] Permission denied for " + department);
//            return null;
//        }

        String key = department + "/" + filename;
        String locationList = fileLocationMap.get(key);
        if (locationList != null) {
            String[] locations = locationList.split(",");
            // Build a list of node indices and their loads for nodes that have the file
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
                // Sort candidate nodes by load (ascending)
                candidateNodes.sort(Comparator.comparingInt(nodeLoads::get));
                // Try each node in order of least load
                for (int nodeId : candidateNodes) {
                    NodeInfo node = nodeInfoMap.get(nodeId);
                    try (Socket socket = new Socket()) {
                        socket.connect(new InetSocketAddress(node.host, node.port), 10000);
                        socket.setSoTimeout(10000);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.flush();
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                        out.writeUTF("fetch");
                        out.writeUTF(department);
                        out.writeUTF(filename);
                        out.flush();
                        byte[] data = (byte[]) in.readObject();
                        if (data != null && data.length > 0) {
                            System.out.println("[COORDINATOR] File " + key + " served from node " + nodeId + " (Load: " + node.currentLoad + ")");
                            System.out.println("File locations for " + key + ": " + fileLocationMap.get(key));
                            return data;
                        }
                    } catch (Exception e) {
                        System.err.println("[COORDINATOR] Node " + nodeId + " error: " + e.getMessage());
                    }
                }
            }
        }
        // Fallback: try all active nodes if not found in fileLocationMap
        System.out.println("[COORDINATOR] Fallback: trying all active nodes for file: " + key);
        for (int i = 0; i < nodeInfoMap.size(); i++) {
            NodeInfo node = nodeInfoMap.get(i);
            if (node != null && node.isActive) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(node.host, node.port), 10000);
                    socket.setSoTimeout(10000);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    out.writeUTF("fetch");
                    out.writeUTF(department);
                    out.writeUTF(filename);
                    out.flush();
                    byte[] data = (byte[]) in.readObject();
                    if (data != null && data.length > 0) {
                        System.out.println("[COORDINATOR] Fallback: file " + key + " found on node " + i);
                        // Update fileLocationMap for future requests
                        fileLocationMap.put(key, node.host + ":" + node.port);
                        System.out.println("File locations for " + key + ": " + fileLocationMap.get(key));
                        return data;
                    }
                } catch (Exception e) {
                    System.err.println("[COORDINATOR] Node " + i + " error: " + e.getMessage());
                }
            }
        }
        System.out.println("[COORDINATOR] File " + key + " not found on any available node");
        return new byte[0];
    }
    @Override
    public List<String> listFiles(String token, String department) throws RemoteException {
//        if (!authService.hasPermission(token, "view", department)) {
//            System.out.println("[COORDINATOR] Permission denied for user to view department: " + department);
//            return List.of("Permission denied");
//        }

        System.out.println("[COORDINATOR] Attempting to list files in " + department);
        List<String> result = new ArrayList<>();

        // Get active nodes
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

        // Try each active node until we get a successful response
        for (int nodeId : activeNodes) {
            NodeInfo node = nodeInfoMap.get(nodeId);
            System.out.println("[COORDINATOR] Trying node " + nodeId + " for listing files");

            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(node.host, node.port), 10000);
                socket.setSoTimeout(10000);
                socket.setKeepAlive(true);
                socket.setTcpNoDelay(true);

                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                System.out.println("[COORDINATOR] Connected to node " + nodeId + " for listing files");

                out.writeUTF("list");
                out.writeUTF(department);
                out.flush();

                // Add retry logic for reading the response
                int retries = 3;
                while (retries > 0) {
                    try {
                        @SuppressWarnings("unchecked")
                        List<String> files = (List<String>) in.readObject();
                        if (files != null) {
                            result.addAll(files);
                            System.out.println("[COORDINATOR] Retrieved " + files.size() + " files from node " + nodeId);
                            if (!result.isEmpty()) {
                                return result; // Return if we found any files
                            }
                        }
                        break; // Break the retry loop if we got a response
                    } catch (Exception e) {
                        retries--;
                        if (retries > 0) {
                            System.out.println("[COORDINATOR] Retrying read from node " + nodeId + " (" + retries + " attempts left)");
                            Thread.sleep(1000);
                            continue;
                        }
                        System.err.println("[COORDINATOR] Error reading from node " + nodeId + ": " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                System.err.println("[COORDINATOR] Error connecting to node " + nodeId + ": " + e.getMessage());
                continue;
            }
        }

        // If we get here, either all nodes failed or no files were found
        if (result.isEmpty()) {
            System.out.println("[COORDINATOR] No files found in any node");
            return List.of("No files found");
        }

        return result;
    }
    @Override
    public synchronized boolean lockFileForEdit(String token, String filename, String department) throws RemoteException {
        String key = department + "/" + filename;
        if (fileEditLocks.containsKey(key)) return false; // Already locked
        fileEditLocks.put(key, token);
        System.out.println("[COORDINATOR] File locked for edit: " + key + " by token " + token);
        return true;
    }
    @Override
    public synchronized boolean unlockFileForEdit(String token, String filename, String department) throws RemoteException {
        String key = department + "/" + filename;
        if (fileEditLocks.getOrDefault(key, "").equals(token)) {
            fileEditLocks.remove(key);
            System.out.println("[COORDINATOR] File unlocked for edit: " + key + " by token " + token);
            return true;
        }
        return false;
    }
}
