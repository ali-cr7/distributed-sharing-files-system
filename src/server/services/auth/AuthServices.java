package server.services.auth;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
public interface AuthServices extends Remote {
    boolean registerUser(String token, String username, String password, String role, String department) throws RemoteException;
    String login(String username, String password) throws RemoteException;
    List<String> listUsers(String token) throws RemoteException;
    boolean hasPermission(String token, String action, String department) throws RemoteException;
}
