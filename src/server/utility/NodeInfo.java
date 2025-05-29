package server.utility;

import java.util.concurrent.atomic.AtomicInteger;

public  class NodeInfo {
    public   String host;
    public   int port;
    public AtomicInteger activeConnections;
  public  boolean isActive;
    public   int currentLoad;
    public   int failureCount;

    public NodeInfo(String host, int port, AtomicInteger activeConnections, boolean isActive) {
        this.host = host;
        this.port = port;
        this.activeConnections = activeConnections;
        this.isActive = isActive;
        this.currentLoad = 0;
        this.failureCount = 0;
    }
}