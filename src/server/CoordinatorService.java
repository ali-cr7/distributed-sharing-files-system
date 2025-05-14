package server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface CoordinatorService extends Remote {
    boolean registerUser(String token, String username, String password, String role, String department) throws RemoteException;

    String login(String username, String password) throws RemoteException;
    boolean hasPermission(String token, String action, String department) throws RemoteException;
    boolean sendFileCommand(String token, String action, String filename, String department, byte[] content) throws RemoteException;
    List<String> listUsers(String token) throws RemoteException;
    byte[]  requestFile(String token, String filename, String department) throws RemoteException;
    List<String> listFiles(String token, String department) throws RemoteException;
    void simulateLoadOnNode(int nodeIndex, int loadAmount) throws RemoteException;
    void setNodeStatus(int nodeIndex, boolean isActive) throws RemoteException;

}

