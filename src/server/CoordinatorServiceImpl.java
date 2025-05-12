package server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

class CoordinatorServiceImpl extends UnicastRemoteObject implements CoordinatorService {
    static class User {
        String username, password, role, department;
        User(String u, String p, String r, String d) {
            username = u; password = p; role = r; department = d;
        }
    }

    private final Map<String, User> users = new ConcurrentHashMap<>();
    private final Map<String, String> activeTokens = new ConcurrentHashMap<>();
    private final List<String> nodeAddresses = List.of("localhost", "localhost", "localhost");
    private final List<Integer> nodePorts = List.of(5001, 5002, 5003);
    private int nodeIndex = 0;

    protected CoordinatorServiceImpl() throws RemoteException {
        super();
    }

    @Override
    public boolean registerUser(String username, String password, String role, String department) throws RemoteException {
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
    public boolean hasPermission(String token, String action, String department) throws RemoteException {
        String username = activeTokens.get(token);
        if (username == null) return false;
        User user = users.get(username);
        if (user == null) return false;
        if (user.role.equals("manager")) return true;
        return user.department.equals(department) && List.of("add", "edit", "delete").contains(action);
    }

    @Override
    public boolean sendFileCommand(String token, String action, String filename, String department, byte[] content) throws RemoteException {
        if (!hasPermission(token, action, department)) return false;

        String nodeHost = nodeAddresses.get(nodeIndex);
        int port = nodePorts.get(nodeIndex);
        nodeIndex = (nodeIndex + 1) % nodeAddresses.size();

        try (Socket socket = new Socket(nodeHost, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeUTF(action);
            out.writeUTF(department);
            out.writeUTF(filename);
            out.writeObject(content);

            return in.readBoolean();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
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
    public byte[] requestFile(String token, String filename, String department) throws RemoteException {
        for (int i = 0; i < nodeAddresses.size(); i++) {
            String nodeHost = nodeAddresses.get(i);
            int port = nodePorts.get(i);

            try (Socket socket = new Socket(nodeHost, port);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                out.writeUTF("fetch");
                out.writeUTF(department);
                out.writeUTF(filename);
                out.flush();

                Object obj = in.readObject();
                if (obj instanceof byte[]) {
                    return (byte[]) obj;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}



