package server.interfaces;

import java.rmi.RemoteException;
import java.util.List;

public interface UserListingService extends CoordinatorService {
    List<String> listUsers(String token) throws RemoteException;
} 