package client;

import server.CoordinatorService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestClient {
    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);
            CoordinatorService service = (CoordinatorService) registry.lookup("CoordinatorService");

            System.out.println("1) Register  2) Login");
            int authChoice = Integer.parseInt(scanner.nextLine());
            String token = null;

            if (authChoice == 1) {
                System.out.print("New Username: ");
                String username = scanner.nextLine();
                System.out.print("New Password: ");
                String password = scanner.nextLine();
                System.out.print("Role (manager/employee): ");
                String role = scanner.nextLine().toLowerCase();
                System.out.print("Department: ");
                String department = scanner.nextLine();

                boolean registered = service.registerUser(username, password, role, department);
                System.out.println(registered ? "Registration successful." : "Registration failed: User may already exist.");
                if (!registered) return;

                token = service.login(username, password);
            } else {
                System.out.print("Username: ");
                String username = scanner.nextLine();
                System.out.print("Password: ");
                String password = scanner.nextLine();
                token = service.login(username, password);
            }

            if (token == null) {
                System.out.println("Login failed.");
                return;
            }

            System.out.println("Login successful. Token: " + token);

            while (true) {
                System.out.println("\nChoose action:");
                System.out.println("1) Send File");
                System.out.println("2) Show Users (Manager only)");
                System.out.println("3) Download File");
                System.out.println("4) Exit");
                System.out.print("Your choice: ");
                int choice = Integer.parseInt(scanner.nextLine());

                switch (choice) {
                    case 1: {
                        System.out.print("Action (add/edit/delete): ");
                        String action = scanner.nextLine().toLowerCase();

                        System.out.print("Filename: ");
                        String filename = scanner.nextLine();

                        System.out.print("Department: ");
                        String department = scanner.nextLine();

                        byte[] content = new byte[0];
                        if (!action.equals("delete")) {
                            System.out.print("File Content: ");
                            content = scanner.nextLine().getBytes();
                        }

                        boolean result = service.sendFileCommand(token, action, filename, department, content);
                        System.out.println("Operation success: " + result);
                        break;
                    }
                    case 2: {
                        List<String> users = service.listUsers(token);
                        users.forEach(System.out::println);
                        break;
                    }
                    case 3: {
                        System.out.print("Filename to fetch: ");
                        String filename = scanner.nextLine();

                        System.out.print("Department: ");
                        String department = scanner.nextLine();

                        byte[] fileData = service.requestFile(token, filename, department);
                        if (fileData == null) {
                            System.out.println("File not found on any node.");
                        } else {
                            Files.write(Paths.get("downloaded_" + filename), fileData, StandardOpenOption.CREATE);
                            System.out.println("File downloaded as 'downloaded_" + filename + "'");
                        }
                        break;
                    }
                    case 4:
                        System.out.println("Goodbye.");
                        return;
                    default:
                        System.out.println("Invalid choice.");
                }
            }

        } catch (Exception e) {
            System.err.println("Client error:");
            e.printStackTrace();
        }
    }
}
