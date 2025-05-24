package server.interfaces;

import java.rmi.RemoteException;

public interface FileLockingService extends CoordinatorService {
    boolean lockFileForEdit(String token, String filename, String department) throws RemoteException;
    boolean unlockFileForEdit(String token, String filename, String department) throws RemoteException;
} 