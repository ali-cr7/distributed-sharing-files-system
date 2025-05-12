package client;

import server.CoordinatorService;

import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestClient {
    private static final List<String> ALLOWED_DEPARTMENTS = List.of("QA", "Graphic", "Development", "general");
    private static String currentUsername = "";
    private static String currentRole = "";

    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);
            CoordinatorService service = (CoordinatorService) registry.lookup("CoordinatorService");

            // Try to login as admin first
            String token = service.login("admin", "admin123");

            // If admin doesn't exist, create it
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
                                handleUserManagement(scanner, service, token);
                            } else {
                                System.out.println("Access denied!");
                            }
                            break;

                        case 4: // List users (admin only)
                            if (currentRole.equals("manager")) {
                                listAllUsers(service, token);
                            } else {
                                System.out.println("Access denied!");
                            }
                            break;

                        case 0: // Logout
                            token = showLoginScreen(scanner, service);
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

    private static String showLoginScreen(Scanner scanner, CoordinatorService service) throws Exception {
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
    private static void handleFileDownloadWithList(Scanner scanner, CoordinatorService service, String token) {
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

    private static void handleFileOperations(Scanner scanner, CoordinatorService service, String token) throws Exception {
        System.out.println("\n=== File Operations ===");
        System.out.print("Action (add/edit/delete/list): ");
        String action = scanner.nextLine().toLowerCase();

        String department;
        do {
            System.out.print("Department (" + String.join("/", ALLOWED_DEPARTMENTS) + "): ");
            department = scanner.nextLine();
        } while (!ALLOWED_DEPARTMENTS.contains(department));

        System.out.print("Filename: ");
        String filename = scanner.nextLine();

        byte[] content = new byte[0];
        if (!action.equals("delete")) {
            System.out.print("File content: ");
            content = scanner.nextLine().getBytes();
        }

        boolean result = service.sendFileCommand(token, action, filename, department, content);
        System.out.println(result ? "Operation successful!" : "Operation failed (check permissions)");
    }



    private static void handleUserManagement(Scanner scanner, CoordinatorService service, String token) throws Exception {
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

    private static void listAllUsers(CoordinatorService service, String token) throws Exception {
        System.out.println("\n=== User List ===");
        List<String> users = service.listUsers(token);
        if (users.isEmpty() || users.get(0).contains("Access Denied")) {
            System.out.println("No users found or access denied!");
        } else {
            users.forEach(System.out::println);
        }
    }
}