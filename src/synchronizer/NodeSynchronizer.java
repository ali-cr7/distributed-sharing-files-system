package synchronizer;

import java.io.*;
import java.nio.file.*;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.*;

public class NodeSynchronizer {
    private final List<String> nodePaths;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private volatile boolean running = true;

    public NodeSynchronizer(List<String> nodePaths) {
        this.nodePaths = new ArrayList<>(nodePaths);
        createMissingDirectories();
    }

    private void createMissingDirectories() {
        for (String nodePath : nodePaths) {
            new File(nodePath).mkdirs();
            for (String otherNode : nodePaths) {
                if (!otherNode.equals(nodePath)) {
                    Path otherPath = Paths.get(otherNode);
                    try {
                        Files.walk(otherPath)
                                .filter(Files::isDirectory)
                                .forEach(dir -> {
                                    Path relative = otherPath.relativize(dir);
                                    Path target = Paths.get(nodePath, relative.toString());
                                    if (!Files.exists(target)) {
                                        target.toFile().mkdirs();
                                    }
                                });
                    } catch (IOException e) {
                        System.err.println("Error creating directories: " + e.getMessage());
                    }
                }
            }
        }
    }

    public void sync(boolean immediate) {
        if (immediate) {
            performSync();
        } else {
            scheduleDailySync();
        }
    }

    private void scheduleDailySync() {
        LocalTime now = LocalTime.now();
        LocalTime midnight = LocalTime.MIDNIGHT.plusHours(1); // 1 AM
        long initialDelay = now.until(midnight, java.time.temporal.ChronoUnit.MINUTES);
        if (initialDelay <= 0) initialDelay += TimeUnit.DAYS.toMinutes(1);

        scheduler.scheduleAtFixedRate(() -> performSync(),
                initialDelay, TimeUnit.DAYS.toMinutes(1), TimeUnit.MINUTES);

        System.out.println("Scheduled daily sync at 1 AM. Next sync in " + initialDelay + " minutes");
    }

    private void performSync() {
        System.out.println("Starting file synchronization...");

        // Build file index across all nodes
        Map<String, FileRecord> fileIndex = new HashMap<>();
        for (String nodePath : nodePaths) {
            indexFiles(nodePath, fileIndex);
        }

        // Synchronize files
        for (Map.Entry<String, FileRecord> entry : fileIndex.entrySet()) {
            FileRecord latest = entry.getValue();
            for (String nodePath : nodePaths) {
                if (!latest.nodePath.equals(nodePath)) {
                    syncFileToNode(latest, nodePath);
                }
            }
        }

        System.out.println("File synchronization completed");
    }

    private void indexFiles(String nodePath, Map<String, FileRecord> index) {
        try {
            Files.walk(Paths.get(nodePath))
                    .filter(Files::isRegularFile)
                    .forEach(file -> {
                        String relativePath = Paths.get(nodePath).relativize(file).toString();
                        long lastModified = file.toFile().lastModified();

                        FileRecord existing = index.get(relativePath);
                        if (existing == null || lastModified > existing.lastModified) {
                            index.put(relativePath, new FileRecord(nodePath, file.toString(), lastModified));
                        }
                    });
        } catch (IOException e) {
            System.err.println("Error indexing files in " + nodePath + ": " + e.getMessage());
        }
    }

    private void syncFileToNode(FileRecord source, String targetNodePath) {
        Path targetPath = Paths.get(targetNodePath,
                Paths.get(source.nodePath).relativize(Paths.get(source.filePath)).toString());

        try {
            // Check if target exists and is older
            if (Files.exists(targetPath)) {
                long targetLastModified = Files.getLastModifiedTime(targetPath).toMillis();
                if (source.lastModified <= targetLastModified) {
                    return; // Target is up-to-date
                }
            }

            // Create parent directories if needed
            Files.createDirectories(targetPath.getParent());

            // Copy file
            Files.copy(Paths.get(source.filePath), targetPath,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.COPY_ATTRIBUTES);

            System.out.println("Synced " + source.filePath + " to " + targetPath);
        } catch (IOException e) {
            System.err.println("Failed to sync " + source.filePath + " to " + targetPath +
                    ": " + e.getMessage());
        }
    }

    public void shutdown() {
        running = false;
        scheduler.shutdown();
    }

    public boolean isRunning() {
        return running;
    }

    private static class FileRecord {
        final String nodePath;
        final String filePath;
        final long lastModified;

        FileRecord(String nodePath, String filePath, long lastModified) {
            this.nodePath = nodePath;
            this.filePath = filePath;
            this.lastModified = lastModified;
        }
    }
}