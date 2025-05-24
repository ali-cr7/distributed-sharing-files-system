package server.implementations;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import server.interfaces.UserListingService;

public class UserListingServiceImpl extends BaseCoordinatorService implements UserListingService {
    
    public UserListingServiceImpl() throws RemoteException {
        super();
    }

    @Override
    public List<String> listUsers(String token) throws RemoteException {
        String username = activeTokens.get(token);
        if (username == null) {
            return List.of("Invalid token");
        }

        User currentUser = users.get(username);
        if (currentUser == null) {
            return List.of("User not found");
        }

        List<String> userList = new ArrayList<>();
        for (User user : users.values()) {
            if (currentUser.role.equals("manager") || 
                (currentUser.role.equals("employee") && currentUser.department.equals(user.department))) {
                userList.add(String.format("%s (%s - %s)", user.username, user.role, user.department));
            }
        }
        return userList;
    }
} 