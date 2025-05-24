package server.interfaces;

import java.rmi.RemoteException;
import java.util.List;

public interface FileOperationService extends CoordinatorService {
    boolean sendFileCommand(String token, String action, String filename, String department, byte[] content) throws RemoteException;
    byte[] requestFile(String token, String filename, String department) throws RemoteException;
    List<String> listFiles(String token, String department) throws RemoteException;
    void updateNodeStatus(int nodeId, boolean isActive) throws RemoteException;
} 