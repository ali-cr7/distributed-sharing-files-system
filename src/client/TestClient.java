package client;

import server.services.auth.AuthServices;
import server.services.file_operations.FileOperationsService;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestClient {
    private static final List<String> ALLOWED_DEPARTMENTS = List.of("QA", "Graphic", "Development", "general");
    private static String currentUsername = "";
    private static String currentRole = "";
    // Track active load connections
    private static final List<LoadConnection> activeConnections = new ArrayList<>();
    private static final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private record LoadConnection(Thread thread, Socket socket) {
    }
    private static void createRealLoad(Scanner scanner, FileOperationsService service) {
        try {
            System.out.print("Enter node port (5001/5002/5003): ");
            int port = Integer.parseInt(scanner.nextLine());

            // ✅ Check if the port is active before continuing
            if (!isPortOpen("localhost", port, 2000)) {
                System.out.println("❌ Node on port " + port + " is not active. Please start the node first.");
                return;
            }

            System.out.print("Number of parallel connections to create: ");
            int connections = Integer.parseInt(scanner.nextLine());

            System.out.print("Duration (seconds, 0 for indefinite): ");
            int duration = Integer.parseInt(scanner.nextLine());

            System.out.println("Creating " + connections + " persistent connections to port " + port + "...");

            stopRequested.set(false);
            clearActiveConnections();

            for (int i = 0; i < connections; i++) {
                final int threadNum = i;
                Thread t = new Thread(() -> {
                    long endTime = duration > 0 ? System.currentTimeMillis() + (duration * 1000) : Long.MAX_VALUE;
                    while (!stopRequested.get() && System.currentTimeMillis() < endTime) {
                        try (Socket socket = new Socket("localhost", port)) {
                            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                            synchronized (activeConnections) {
                                activeConnections.add(new LoadConnection(Thread.currentThread(), socket));
                            }
                            while (!stopRequested.get() && System.currentTimeMillis() < endTime) {
                                out.writeUTF("ping");
                                out.flush();
                                String resp = in.readUTF();
                                if (!"pong".equals(resp)) break;
                                Thread.sleep(2000);
                            }
                        } catch (Exception e) {
                            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
                        } finally {
                            removeConnection(Thread.currentThread());
                        }
                    }
                }, "LoadThread-" + threadNum);
                t.start();
            }

            System.out.println("✅ Load generation started. Use option 6 to stop early.");
        } catch (Exception e) {
            System.out.println("❌ Error creating load: " + e.getMessage());
        }
    }
    private static boolean isPortOpen(String host, int port, int timeoutMillis) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMillis);
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    private static void clearActiveConnections() {
        synchronized (activeConnections) {
            for (LoadConnection conn : activeConnections) {
                try {
                    if (!conn.socket.isClosed()) {
                        conn.socket.close();
                    }
                } catch (IOException e) {
                    System.out.println("Error closing socket: " + e.getMessage());
                }
                conn.thread.interrupt();
            }
            activeConnections.clear();
        }
    }
    private static void removeConnection(Thread thread) {
        synchronized (activeConnections) {
            activeConnections.removeIf(conn -> conn.thread == thread);
        }
    }
    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);
            FileOperationsService service = (FileOperationsService) registry.lookup("CoordinatorService");
            AuthServices authServices = (AuthServices) registry.lookup("AuthServices");


            // Try to login as admin first
            String token = authServices.login("admin", "admin123");

            // If admin doesn't exist, create it
            if (token == null) {
                System.out.println("Admin account not found. Creating default admin...");
                boolean created = authServices.registerUser("", "admin", "admin123", "manager", "general");
                if (created) {
                    token = authServices.login("admin", "admin123");
                    System.out.println("Default admin created successfully!");
                } else {
                    System.out.println("Failed to create admin account!");
                    return;
                }
            }

            // Set current user info
            currentUsername = "admin";
            currentRole = "manager";

            // Main application loop
            while (true) {

                System.out.println("\n===== Main Menu ===== [" + currentUsername + " - " + currentRole + "]");
                System.out.println("1) File Operations");
                System.out.println("2) Download File");

                if (currentRole.equals("manager")) {
                    System.out.println("3) User Management");
                    System.out.println("4) List All Users");
                    System.out.println("5) Create Real Load (Socket Connections)");

                }

                System.out.println("0) Logout");
                System.out.print("Your choice: ");

                try {
                    int choice = Integer.parseInt(scanner.nextLine());

                    switch (choice) {
                        case 1: // File operations
                            handleFileOperations(scanner, service, token);
                            break;

                        case 2: // Download file
                            handleFileDownloadWithList(scanner, service, token);
                            break;

                        case 3: // User management (admin only)
                            if (currentRole.equals("manager")) {
                                handleUserManagement(scanner, authServices, token);
                            } else {
                                System.out.println("Access denied!");
                            }
                            break;

                        case 4: // List users (admin only)
                            if (currentRole.equals("manager")) {
                                listAllUsers(authServices, token);
                            } else {
                                System.out.println("Access denied!");
                            }
                            break;
                        case 5:
                            if (currentRole.equals("manager")) {
                                createRealLoad(scanner, service);
                            }
                            break;

                        case 0: // Logout
                            token = showLoginScreen(scanner, authServices);
                            if (token == null) return;
                            break;

                        default:
                            System.out.println("Invalid choice!");
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Please enter a valid number!");
                }
            }
        } catch (Exception e) {
            System.err.println("Client error:");
            e.printStackTrace();
        }
    }
    private static String showLoginScreen(Scanner scanner, AuthServices service) throws Exception {
        while (true) {
            System.out.println("\n=== Distributed File System ===");
            System.out.println("1) Admin Login");
            System.out.println("2) Employee Login");
            System.out.println("3) Exit");
            System.out.print("Your choice: ");

            String choice = scanner.nextLine();

            switch (choice) {
                case "1": // Admin login
                    System.out.print("Admin username: ");
                    String adminUser = scanner.nextLine();
                    System.out.print("Password: ");
                    String adminPass = scanner.nextLine();

                    String adminToken = service.login(adminUser, adminPass);
                    if (adminToken != null) {
                        currentUsername = adminUser;
                        currentRole = "manager";
                        return adminToken;
                    }
                    System.out.println("Invalid admin credentials!");
                    break;

                case "2": // Employee login
                    System.out.print("Username: ");
                    String empUser = scanner.nextLine();
                    System.out.print("Password: ");
                    String empPass = scanner.nextLine();

                    String empToken = service.login(empUser, empPass);
                    if (empToken != null) {
                        currentUsername = empUser;
                        currentRole = "employee";
                        return empToken;
                    }
                    System.out.println("Invalid credentials or account doesn't exist!");
                    break;

                case "3": // Exit
                    return null;

                default:
                    System.out.println("Invalid choice!");
            }
        }
    }
    private static void handleFileDownloadWithList(Scanner scanner, FileOperationsService service, String token) {
        try {
            // Get department
            String department;
            do {
                System.out.print("Department (" + String.join("/", ALLOWED_DEPARTMENTS) + "): ");
                department = scanner.nextLine();
            } while (!ALLOWED_DEPARTMENTS.contains(department));

            // List available files
            System.out.println("\nFetching available files...");
            List<String> files = service.listFiles(token, department);

            if (files.isEmpty()) {
                System.out.println("No files found in " + department + " department.");
                System.out.println("Check if:");
                System.out.println("1. Files exist in the nodes' storage directories");
                System.out.println("2. You have proper permissions");
                return;
            }

            // Display file list
            System.out.println("\nAvailable files in " + department + ":");
            for (int i = 0; i < files.size(); i++) {
                System.out.println((i+1) + ") " + files.get(i));
            }

            // Get file selection
            int selection;
            do {
                System.out.print("\nSelect file (1-" + files.size() + "): ");
                try {
                    selection = Integer.parseInt(scanner.nextLine());
                } catch (NumberFormatException e) {
                    selection = -1;
                }
            } while (selection < 1 || selection > files.size());

            String filename = files.get(selection-1);
            System.out.println("Downloading " + filename + "...");

            // Download file
            byte[] fileData = service.requestFile(token, filename, department);

            if (fileData == null || fileData.length == 0) {
                System.out.println("Failed to download file. Possible reasons:");
                System.out.println("- File was deleted recently");
                System.out.println("- Network issues");
                System.out.println("- Permission denied");
            } else {
                String outputFilename = "downloaded_" + filename;
                Files.write(Paths.get(outputFilename), fileData);
                System.out.println("\nFile saved as: " + outputFilename);

                // Display text file content
                if (filename.endsWith(".txt")) {
                    System.out.println("\n--- FILE CONTENT ---");
                    System.out.println(new String(fileData));
                    System.out.println("--- END OF CONTENT ---");
                }
            }
        } catch (Exception e) {
            System.out.println("Error during download: " + e.getMessage());
        }
    }
    private static void handleFileOperations(Scanner scanner, FileOperationsService service, String token) throws Exception {
        System.out.println("\n=== File Operations ===");
        System.out.print("Action (add/edit/delete/list): ");
        String action = scanner.nextLine().toLowerCase();

        String department;
        do {
            System.out.print("Department (" + String.join("/", ALLOWED_DEPARTMENTS) + "): ");
            department = scanner.nextLine();
        } while (!ALLOWED_DEPARTMENTS.contains(department));

        if (action.equals("list")) {
            List<String> files = service.listFiles(token, department);
            if (files.isEmpty()) {
                System.out.println("No files found in " + department + " department.");
                return;
            }
            System.out.println("\nAvailable files in " + department + ":");
            for (int i = 0; i < files.size(); i++) {
                System.out.println((i+1) + ") " + files.get(i));
            }
            return;
        }

        if (action.equals("edit")) {
            // List available files
            List<String> files = service.listFiles(token, department);
            if (files.isEmpty()) {
                System.out.println("No files found in " + department + " department.");
                return;
            }

            System.out.println("\nAvailable files in " + department + ":");
            for (int i = 0; i < files.size(); i++) {
                System.out.println((i+1) + ") " + files.get(i));
            }

            // Get file selection
            int selection;
            do {
                System.out.print("\nSelect file to edit (1-" + files.size() + "): ");
                try {
                    selection = Integer.parseInt(scanner.nextLine());
                } catch (NumberFormatException e) {
                    selection = -1;
                }
            } while (selection < 1 || selection > files.size());

            String filename = files.get(selection-1);

            // Try to lock the file for editing
            if (!service.lockFileForEdit(token, filename, department)) {
                System.out.println("File is currently being edited by another user. Try again later.");
                return;
            }

            try {
                // Show current content
                System.out.println("\n=== Current File Content ===");
                byte[] currentContent = service.requestFile(token, filename, department);
                if (currentContent != null && currentContent.length > 0) {
                    System.out.println(new String(currentContent));
                } else {
                    System.out.println("(Empty file)");
                }
                System.out.println("=== End of Current Content ===\n");

                // Get new content
                System.out.println("Enter new content (type 'END' on a new line to finish):");
                System.out.println("----------------------------------------");
                StringBuilder newContent = new StringBuilder();
                String line;
                while (!(line = scanner.nextLine()).equals("END")) {
                    newContent.append(line).append("\n");
                }
                System.out.println("----------------------------------------");

                // Confirm edit
                System.out.print("\nDo you want to save these changes? (yes/no): ");
                String confirm = scanner.nextLine().toLowerCase();
                if (!confirm.equals("yes")) {
                    System.out.println("Edit cancelled.");
                    return;
                }

                // Send edit command
                boolean result = service.sendFileCommand(token, "edit", filename, department,
                        newContent.toString().getBytes());
                System.out.println(result ? "File edited successfully!" : "Edit operation failed!");
            } finally {
                service.unlockFileForEdit(token, filename, department);
            }
            return;
        }

        // For add and delete operations
        System.out.print("Filename: ");
        String filename = scanner.nextLine();

        byte[] content = new byte[0];
        if (action.equals("add")) {
            System.out.println("Enter file content (type 'END' on a new line to finish):");
            System.out.println("----------------------------------------");
            StringBuilder fileContent = new StringBuilder();
            String line;
            while (!(line = scanner.nextLine()).equals("END")) {
                fileContent.append(line).append("\n");
            }
            System.out.println("----------------------------------------");
            content = fileContent.toString().getBytes();
        }

        boolean result = service.sendFileCommand(token, action, filename, department, content);
        System.out.println(result ? "Operation successful!" : "Operation failed (check permissions)");
    }
    private static void handleUserManagement(Scanner scanner, AuthServices service, String token) throws Exception {
        System.out.println("\n=== User Management ===");
        System.out.print("Username: ");
        String username = scanner.nextLine();
        System.out.print("Password: ");
        String password = scanner.nextLine();
        System.out.print("Role (manager/employee): ");
        String role = scanner.nextLine().toLowerCase();

        String department;
        do {
            System.out.print("Department (" + String.join("/", ALLOWED_DEPARTMENTS) + "): ");
            department = scanner.nextLine();
        } while (!ALLOWED_DEPARTMENTS.contains(department));

        boolean success = service.registerUser(token, username, password, role, department);
        System.out.println(success ? "User registered successfully!" : "Registration failed!");
    }
    private static void listAllUsers(AuthServices service, String token) throws Exception {
        System.out.println("\n=== User List ===");
        List<String> users = service.listUsers(token);
        if (users.isEmpty() || users.get(0).contains("Access Denied")) {
            System.out.println("No users found or access denied!");
        } else {
            users.forEach(System.out::println);
        }
    }
}