package company.vk.edu.distrib.compute.dkoften.consensus;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Bonus visualizer: periodically prints a colour-coded cluster topology to the console.
 *
 * <p>ANSI escape codes are used to highlight the node roles:
 * <ul>
 *   <li><b>yellow</b> – LEADER</li>
 *   <li>green – FOLLOWER</li>
 *   <li>red – DOWN</li>
 * </ul>
 */
@SuppressWarnings("PMD.SystemPrintln")
public final class ClusterVisualizer {

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_YELLOW = "\u001B[1;33m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RED = "\u001B[31m";

    private static final String TOPOLOGY_HEADER =
            "\n╔═══════════════════════════╗\n"
            + "║     CLUSTER TOPOLOGY      ║\n"
            + "╠═══════════════════════════╣\n";

    private final LeaderElectionCluster cluster;
    private final long intervalMs;
    private final ScheduledExecutorService scheduler;

    /**
     * Creates a visualizer for the given cluster.
     *
     * @param cluster the cluster whose state will be displayed
     * @param intervalMs refresh interval in milliseconds
     */
    public ClusterVisualizer(LeaderElectionCluster cluster, long intervalMs) {
        this.cluster = cluster;
        this.intervalMs = intervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("visualizer").factory());
    }

    /** Starts periodic cluster-state output to standard output. */
    public void start() {
        scheduler.scheduleAtFixedRate(this::printState, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    /** Stops the visualizer and releases its scheduler. */
    public void stop() {
        scheduler.shutdownNow();
    }

    private void printState() {
        List<ClusterNode> nodeList = cluster.getNodes();
        StringBuilder sb = new StringBuilder(256).append(TOPOLOGY_HEADER);
        for (ClusterNode node : nodeList) {
            NodeState nodeState = node.getState();
            String info = String.format("Node %-2d %-8s ldr=%-2d",
                    node.getNodeId(), labelFor(nodeState), node.getLeaderId());
            sb.append("║ ").append(colorFor(nodeState)).append(info).append(ANSI_RESET).append(" ║\n");
        }
        sb.append("╚═══════════════════════════╝");
        System.out.println(sb);
    }

    private static String colorFor(NodeState nodeState) {
        return switch (nodeState) {
            case LEADER -> ANSI_YELLOW;
            case FOLLOWER -> ANSI_GREEN;
            case DOWN -> ANSI_RED;
        };
    }

    private static String labelFor(NodeState nodeState) {
        return switch (nodeState) {
            case LEADER -> "LEADER";
            case FOLLOWER -> "FOLLOWER";
            case DOWN -> "DOWN";
        };
    }
}
