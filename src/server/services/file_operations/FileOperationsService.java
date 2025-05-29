package server.services.file_operations;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface FileOperationsService extends Remote {
    boolean sendFileCommand(String token, String action, String filename, String department, byte[] content) throws RemoteException;
    byte[]  requestFile(String token, String filename, String department) throws RemoteException;
    List<String> listFiles(String token, String department) throws RemoteException;
    boolean lockFileForEdit(String token, String filename, String department) throws RemoteException;
    boolean unlockFileForEdit(String token, String filename, String department) throws RemoteException;
}

