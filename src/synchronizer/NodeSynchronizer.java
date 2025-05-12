package synchronizer;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class NodeSynchronizer {
    private final List<String> nodeAddresses;
    private final List<Integer> nodePorts;
    private final String basePath;

    public NodeSynchronizer(List<String> nodeAddresses, List<Integer> nodePorts, String basePath) {
        this.nodeAddresses = nodeAddresses;
        this.nodePorts = nodePorts;
        this.basePath = basePath;
    }

    public void synchronize() {
        for (int i = 0; i < nodePorts.size(); i++) {
            File baseDir = new File(basePath  + (i + 1));
            if (!baseDir.exists() || !baseDir.isDirectory()) continue;

            File[] departmentDirs = baseDir.listFiles(File::isDirectory);
            if (departmentDirs == null) continue;

            for (File deptDir : departmentDirs) {
                File[] files = deptDir.listFiles();
                if (files == null) continue;

                for (File file : files) {
                    byte[] content = readFileBytes(file);
                    String department = deptDir.getName();
                    String filename = file.getName();

                    for (int j = 0; j < nodePorts.size(); j++) {
                        if (j == i) continue; // skip self

                        try (Socket socket = new Socket(nodeAddresses.get(j), nodePorts.get(j));
                             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                            out.writeUTF("add");
                            out.writeUTF(department);
                            out.writeUTF(filename);
                            out.writeObject(content);

                            boolean ok = in.readBoolean();
                            System.out.println("Synced " + filename + " from node" + (i + 1) + " to node" + (j + 1) + ": " + ok);
                        } catch (Exception e) {
                            System.err.println("Failed to sync from node" + (i + 1) + " to node" + (j + 1) + ": " + e.getMessage());
                        }
                    }
                }
            }
        }
    }

    private byte[] readFileBytes(File file) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             FileInputStream fis = new FileInputStream(file)) {

            byte[] buffer = new byte[1024];
            int read;
            while ((read = fis.read(buffer)) != -1) {
                baos.write(buffer, 0, read);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }
}
