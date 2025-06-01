package synchronizer;

import java.util.*;
import java.util.Scanner;

public class SyncRunner {
    public static void main(String[] args) {
        List<String> nodePaths = Arrays.asList(
                "node1",
                "node2",
                "node3"
        );

        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("File Synchronization Options:");
            System.out.println("1. Sync now");
            System.out.println("2. Schedule daily sync at 1 AM");
            System.out.print("Enter choice: ");

            int choice = scanner.nextInt();
            boolean immediate = (choice == 1);

            NodeSynchronizer syncManager = new NodeSynchronizer(nodePaths);
            syncManager.sync(immediate);

            if (!immediate) {
                System.out.println("Press Enter to exit...");
                scanner.nextLine(); // Clear buffer
                scanner.nextLine(); // Wait for enter
                syncManager.shutdown();
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}