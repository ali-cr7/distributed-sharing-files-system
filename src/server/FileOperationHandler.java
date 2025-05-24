package server;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class FileOperationHandler {
    private final File baseDir;
    private final Map<String, ReentrantReadWriteLock> fileLocks = new ConcurrentHashMap<>();

    public FileOperationHandler(File baseDir) {
        this.baseDir = baseDir;
    }

    public void handleListAction(ObjectOutputStream out, String department) throws IOException {
        File deptDir = new File(baseDir, department);
        List<String> files = new ArrayList<>();

        System.out.println("[NODE] Listing files in directory: " + deptDir.getAbsolutePath());

        if (!deptDir.exists()) {
            boolean created = deptDir.mkdirs();
            System.out.println("[NODE] " + (created ? "Created" : "Failed to create") + " directory: " + deptDir.getAbsolutePath());
        }

        File[] listFiles = deptDir.listFiles();
        if (listFiles != null) {
            for (File f : listFiles) {
                if (f.isFile()) {
                    String fileKey = department + "/" + f.getName();
                    ReentrantReadWriteLock lock = fileLocks.computeIfAbsent(fileKey, k -> new ReentrantReadWriteLock());

                    lock.readLock().lock();
                    try {
                        files.add(f.getName());
                        System.out.println("[NODE] Found file: " + f.getName() + " (size: " + f.length() + " bytes)");
                    } finally {
                        lock.readLock().unlock();
                    }
                }
            }
        }

        System.out.println("[NODE] Sending " + files.size() + " files to client");
        out.writeObject(files);
        out.flush();
        System.out.println("[NODE] Successfully sent file list to client");
    }

    public void handleAddEditAction(ObjectInputStream in, ObjectOutputStream out, String department, String filename) throws Exception {
        String fileKey = department + "/" + filename;
        ReentrantReadWriteLock lock = fileLocks.computeIfAbsent(fileKey, k -> new ReentrantReadWriteLock());

        try {
            System.out.println("[NODE] Starting " + (new File(new File(baseDir, department), filename).exists() ? "edit" : "add") +
                    " operation for " + fileKey);

            lock.writeLock().lock();
            try {
                byte[] content = (byte[]) in.readObject();
                if (content == null) {
                    System.out.println("[NODE] Received null content for file operation");
                    out.writeBoolean(false);
                    out.flush();
                    return;
                }

                File deptDir = new File(baseDir, department);
                if (!deptDir.exists()) {
                    boolean created = deptDir.mkdirs();
                    System.out.println("[NODE] " + (created ? "Created" : "Failed to create") + " directory: " + deptDir.getAbsolutePath());
                }

                File file = new File(deptDir, filename);
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    fos.write(content);
                    out.writeBoolean(true);
                    out.flush();
                    System.out.println("[NODE] File " + filename + " saved successfully");
                } catch (IOException e) {
                    System.err.println("[NODE] Error writing file " + filename + ": " + e.getMessage());
                    out.writeBoolean(false);
                    out.flush();
                }
            } finally {
                lock.writeLock().unlock();
            }
        } catch (Exception e) {
            System.err.println("[NODE] Error in handleAddEditAction: " + e.getMessage());
            e.printStackTrace();
            try {
                out.writeBoolean(false);
                out.flush();
            } catch (IOException ex) {
                System.err.println("[NODE] Error sending failure response: " + ex.getMessage());
            }
        }
    }

    public void handleDeleteAction(ObjectOutputStream out, String department, String filename) throws IOException {
        String fileKey = department + "/" + filename;
        ReentrantReadWriteLock lock = fileLocks.computeIfAbsent(fileKey, k -> new ReentrantReadWriteLock());

        lock.writeLock().lock();
        try {
            File file = new File(new File(baseDir, department), filename);
            boolean deleted = file.exists() && file.delete();
            out.writeBoolean(deleted);
            System.out.println("[NODE] Delete " + filename + " result: " + deleted);

            if (deleted) {
                fileLocks.remove(fileKey);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void handleFetchAction(ObjectOutputStream out, String department, String filename) throws IOException {
        String fileKey = department + "/" + filename;
        ReentrantReadWriteLock lock = fileLocks.computeIfAbsent(fileKey, k -> new ReentrantReadWriteLock());

        System.out.println("[NODE] Fetch request for: " + fileKey);
        File deptDir = new File(baseDir, department);
        File targetFile = new File(deptDir, filename);

        lock.readLock().lock();
        try {
            if (targetFile.exists() && targetFile.isFile()) {
                try {
                    byte[] data = Files.readAllBytes(targetFile.toPath());
                    System.out.println("[NODE] Sending " + data.length + " bytes");
                    out.writeObject(data);
                    out.flush();
                    System.out.println("[NODE] Successfully sent file data");
                } catch (IOException e) {
                    System.err.println("[NODE] Error reading file: " + e.getMessage());
                    out.writeObject(new byte[0]);
                    out.flush();
                }
            } else {
                System.out.println("[NODE] File not found");
                out.writeObject(new byte[0]);
                out.flush();
            }
        } finally {
            lock.readLock().unlock();
        }
    }
} 