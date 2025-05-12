package synchronizer;

import java.util.List;

public class SyncRunner {
    public static void main(String[] args) {
        List<String> addresses = List.of("localhost", "localhost", "localhost");
        List<Integer> ports = List.of(5001, 5002, 5003);
        String basePath = "node";

        new NodeSynchronizer(addresses, ports, basePath).synchronize();
    }
}