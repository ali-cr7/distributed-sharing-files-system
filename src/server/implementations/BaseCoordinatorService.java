package server.implementations;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import server.interfaces.CoordinatorService;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import server.interfaces.CoordinatorService;

public abstract class BaseCoordinatorService extends UnicastRemoteObject implements CoordinatorService {
    protected static final Map<String, User> users = new ConcurrentHashMap<>();
    protected static final Map<String, String> activeTokens = new ConcurrentHashMap<>();
    protected final Map<String, String> fileLocationMap = new ConcurrentHashMap<>();
    protected static final Map<Integer, NodeInfo> nodeInfoMap = new ConcurrentHashMap<>();
    protected final Map<String, String> fileEditLocks = new ConcurrentHashMap<>();

    protected static class User {
        String username, password, role, department;
        User(String u, String p, String r, String d) {
            username = u; password = p; role = r; department = d;
        }
    }

    protected static class NodeInfo {
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

    protected BaseCoordinatorService() throws RemoteException {
        super();
    }
} 