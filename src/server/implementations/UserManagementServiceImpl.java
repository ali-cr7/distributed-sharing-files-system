package server.implementations;

import java.rmi.RemoteException;
import java.util.List;
import server.interfaces.UserManagementService;

public class UserManagementServiceImpl extends BaseCoordinatorService implements UserManagementService {
    
    public UserManagementServiceImpl() throws RemoteException {
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
        String token = java.util.UUID.randomUUID().toString();
        activeTokens.put(token, username);
        return token;
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