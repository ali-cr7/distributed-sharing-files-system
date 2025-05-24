package server.interfaces;

import java.rmi.RemoteException;

public interface UserManagementService extends CoordinatorService {
    boolean registerUser(String token, String username, String password, String role, String department) throws RemoteException;
    String login(String username, String password) throws RemoteException;
    boolean hasPermission(String token, String action, String department) throws RemoteException;
} 