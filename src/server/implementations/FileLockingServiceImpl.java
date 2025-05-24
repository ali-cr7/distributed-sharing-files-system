package server.implementations;

import java.rmi.RemoteException;
import server.interfaces.FileLockingService;

public class FileLockingServiceImpl extends BaseCoordinatorService implements FileLockingService {
    
    public FileLockingServiceImpl() throws RemoteException {
        super();
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