package server.utility;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class LoadBalancer {
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