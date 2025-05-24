package server;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class ConnectionManager {
    private final AtomicInteger activeConnections;
    private final Set<Socket> activeSockets;
    private final Map<Socket, Long> connectionTimestamps;
    private final Map<Socket, Thread> socketThreads;
    private final Map<Socket, Boolean> validConnections;
    private final long connectionTimeout;

    public ConnectionManager(AtomicInteger activeConnections, Set<Socket> activeSockets,
                           Map<Socket, Long> connectionTimestamps, Map<Socket, Thread> socketThreads,
                           Map<Socket, Boolean> validConnections, long connectionTimeout) {
        this.activeConnections = activeConnections;
        this.activeSockets = activeSockets;
        this.connectionTimestamps = connectionTimestamps;
        this.socketThreads = socketThreads;
        this.validConnections = validConnections;
        this.connectionTimeout = connectionTimeout;
    }

    public void registerValidConnection(Socket socket) {
        validConnections.put(socket, true);
        activeSockets.add(socket);
        connectionTimestamps.put(socket, System.currentTimeMillis());
    }

    public void cleanupConnection(Socket socket) {
        try {
            if (validConnections.getOrDefault(socket, false)) {
                activeSockets.remove(socket);
                if (activeConnections.get() > 0) {
                    activeConnections.decrementAndGet();
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

    public void startCleanupThread() {
        Thread cleanupThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long currentTime = System.currentTimeMillis();
                    connectionTimestamps.entrySet().removeIf(entry -> {
                        if (currentTime - entry.getValue() > connectionTimeout) {
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
}