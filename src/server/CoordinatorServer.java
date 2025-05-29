package server;

import server.services.file_operations.FileOperationsService;
import server.services.file_operations.FileOperationsServiceImpl;
import server.services.auth.AuthServices;
import server.services.auth.AuthServicesImpl;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class CoordinatorServer {
    public static void main(String[] args) {
        try {

            AuthServices authServices = new AuthServicesImpl();
            FileOperationsService service = new FileOperationsServiceImpl(authServices);
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind("CoordinatorService", service);
            registry.rebind("AuthServices",authServices );
            System.out.println("Coordinator RMI Server is running...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}