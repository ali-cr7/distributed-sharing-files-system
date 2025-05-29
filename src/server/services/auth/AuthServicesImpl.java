package server.services.auth;
import server.utility.User;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
public class AuthServicesImpl   extends UnicastRemoteObject implements AuthServices {
    private final Map<String, User> users = new ConcurrentHashMap<>();
    public final Map<String, String> activeTokens = new ConcurrentHashMap<>();
    public AuthServicesImpl() throws RemoteException {
        super();
    }
    @Override
    public boolean registerUser(String token, String username, String password, String role, String department) throws RemoteException {
        if (users.isEmpty() && username.equals("admin") && role.equals("manager")) {
            users.put(username, new User(username, password, role, department));
            return true;
        }

        List<String> allowedDepartments = List.of("QA", "Graphic", "Development", "general");
        if (!allowedDepartments.contains(department)) return false;

        String requester = activeTokens.get(token);
        if (requester == null) return false;

        User currentUser = users.get(requester);
        if (currentUser == null || !currentUser.role.equals("manager")) return false;
        if (users.containsKey(username)) return false;

        users.put(username, new User(username, password, role, department));
        return true;
    }
    @Override
    public String login(String username, String password) throws RemoteException {
        User user = users.get(username);
        if (user == null || !user.password.equals(password)) return null;
        String token = UUID.randomUUID().toString();
        activeTokens.put(token, username);
        return token;
    }
    @Override
    public List<String> listUsers(String token) throws RemoteException {
        String username = activeTokens.get(token);
        if (username == null) return List.of("Access Denied: Invalid token");
        User user = users.get(username);
        if (!"manager".equals(user.role)) return List.of("Access Denied: Not a manager");

        List<String> result = new ArrayList<>();
        for (User u : users.values()) {
            result.add("Username: " + u.username + ", Role: " + u.role + ", Department: " + u.department);
        }
        return result;
    }
    @Override
    public boolean hasPermission(String token, String action, String department) throws RemoteException {
        String username = activeTokens.get(token);
        if (username == null) return false;
        User user = users.get(username);
        if (user == null) return false;
        if (user.role.equals("manager")) return true;
        return user.department.equals(department) && List.of("add", "edit", "delete", "view").contains(action);
    }
}
