package test;

import server.CoordinatorService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class LoadBalancingTest {
    public static void main(String[] args) throws Exception {
        String department = "QA"; // Change as needed
        String filename = "ali33"; // Change as needed
        int numThreads = 10; // Number of parallel download requests

        Registry registry = LocateRegistry.getRegistry("localhost", 1099);
        CoordinatorService service = (CoordinatorService) registry.lookup("CoordinatorService");

        // Login as admin (or use a valid token)
        String token = service.login("admin", "admin123");
        if (token == null) {
            System.out.println("Admin account not found. Creating default admin...");
            boolean created = service.registerUser("", "admin", "admin123", "manager", "general");
            if (created) {
                token = service.login("admin", "admin123");
                System.out.println("Default admin created successfully!");
            } else {
                System.out.println("Failed to create admin account!");
                return;
            }
        }
        String finalToken = token;
        Runnable downloadTask = () -> {
            try {
                byte[] data = service.requestFile(finalToken, filename, department);
                if (data != null && data.length > 0) {
                    System.out.println(Thread.currentThread().getName() + ": Downloaded " + filename + " (" + data.length + " bytes)");
                } else {
                    System.out.println(Thread.currentThread().getName() + ": Failed to download " + filename);
                }
            } catch (Exception e) {
                System.out.println(Thread.currentThread().getName() + ": Exception: " + e.getMessage());
            }
        };

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(downloadTask, "Client-" + (i + 1));
            threads[i].start();
        }
        for (Thread t : threads) t.join();

        System.out.println("All downloads requests completed. Check coordinator logs for which node served each request.");
    }
}