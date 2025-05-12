package server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class CoordinatorServer {
    public static void main(String[] args) {
        try {
            CoordinatorService service = new CoordinatorServiceImpl();
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind("CoordinatorService", service);
            System.out.println("Coordinator RMI Server is running...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}