package server;

import java.net.Socket;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class LoadMonitor {
    private final AtomicInteger activeConnections;
    private final Set<Socket> loadTestSockets;

    public LoadMonitor(AtomicInteger activeConnections, Set<Socket> loadTestSockets) {
        this.activeConnections = activeConnections;
        this.loadTestSockets = loadTestSockets;
    }

    public void incrementLoad() {
        int currentLoad = activeConnections.incrementAndGet();
        System.out.println("[NODE] New valid connection. Current load: " + currentLoad);
    }

    public int getCurrentLoad() {
        return activeConnections.get();
    }
}