package server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import server.implementations.UserManagementServiceImpl;
import server.implementations.FileOperationServiceImpl;
import server.implementations.FileLockingServiceImpl;
import server.implementations.UserListingServiceImpl;
import server.interfaces.UserManagementService;
import server.interfaces.FileOperationService;
import server.interfaces.FileLockingService;
import server.interfaces.UserListingService;

public class CoordinatorServer {
    public static void main(String[] args) {
        try {
            // Create service implementations
            UserManagementService userService = new UserManagementServiceImpl();
            FileOperationService fileService = new FileOperationServiceImpl();
            FileLockingService lockService = new FileLockingServiceImpl();
            UserListingService listingService = new UserListingServiceImpl();

            // Create and bind services to registry
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind("UserManagementService", userService);
            registry.rebind("FileOperationService", fileService);
            registry.rebind("FileLockingService", lockService);
            registry.rebind("UserListingService", listingService);

            System.out.println("Coordinator RMI Server is running with the following services:");
            System.out.println("- UserManagementService");
            System.out.println("- FileOperationService");
            System.out.println("- FileLockingService");
            System.out.println("- UserListingService");
        } catch (Exception e) {
            System.err.println("Error starting Coordinator Server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}