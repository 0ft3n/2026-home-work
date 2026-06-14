package company.vk.edu.distrib.compute.dkoften.consensus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Manages a fixed-size cluster of {@link ClusterNode}s that elect a leader
 * using the Bully algorithm.
 *
 * <p>Nodes are assigned integer IDs starting at {@code 1}. The alive node
 * with the highest ID always wins the election.
 */
public final class LeaderElectionCluster {

    private final List<ClusterNode> nodes;

    /**
     * Creates and wires a cluster of {@code size} nodes with IDs 1..size.
     * Nodes are NOT started; call {@link #start()} explicitly.
     *
     * @param size the number of nodes in the cluster (must be positive)
     */
    public LeaderElectionCluster(int size) {
        List<ClusterNode> list = new ArrayList<>(size);
        for (int idx = 1; idx <= size; idx++) {
            list.add(new ClusterNode(idx));
        }
        for (ClusterNode node : list) {
            node.setPeers(list);
        }
        this.nodes = Collections.unmodifiableList(list);
    }

    /**
     * Starts all cluster nodes.
     * Each node begins its virtual thread and immediately triggers an election.
     */
    public void start() {
        for (ClusterNode node : nodes) {
            node.start();
        }
    }

    /**
     * Stops all cluster nodes, interrupting their processing threads.
     */
    public void stop() {
        for (ClusterNode node : nodes) {
            node.stop();
        }
    }

    /**
     * Returns the node with the given ID (1-based).
     *
     * @param nodeId the node ID to look up
     * @return the corresponding {@link ClusterNode}
     * @throws IndexOutOfBoundsException if {@code nodeId} is out of range
     */
    public ClusterNode getNode(int nodeId) {
        return nodes.get(nodeId - 1);
    }

    /**
     * Returns an unmodifiable view of all nodes in the cluster.
     *
     * @return list of all cluster nodes
     */
    public List<ClusterNode> getNodes() {
        return nodes;
    }

    /**
     * Scans for the node that currently considers itself the leader.
     *
     * @return the leader node ID, or {@code -1} if no leader has been elected yet
     */
    public int getCurrentLeaderId() {
        for (int ii = nodes.size() - 1; ii >= 0; ii--) {
            ClusterNode node = nodes.get(ii);
            if (node.getState() == NodeState.LEADER) {
                return node.getNodeId();
            }
        }
        return -1;
    }

    /**
     * Checks whether all alive nodes agree on the same leader, the agreed leader is
     * alive and has {@link NodeState#LEADER} state.
     *
     * @return {@code true} when full, consistent consensus has been reached
     */
    public boolean hasConsensus() {
        int agreedLeader = -1;
        for (ClusterNode node : nodes) {
            if (!node.isAlive()) {
                continue;
            }
            int nodeLeader = node.getLeaderId();
            if (nodeLeader < 0) {
                return false;
            }
            if (agreedLeader < 0) {
                agreedLeader = nodeLeader;
            } else if (agreedLeader != nodeLeader) {
                return false;
            }
        }
        if (agreedLeader < 0) {
            return false;
        }
        // Verify the agreed-upon leader is actually alive and in LEADER state
        ClusterNode leaderNode = getNode(agreedLeader);
        return leaderNode.isAlive() && leaderNode.getState() == NodeState.LEADER;
    }

    /**
     * Returns the ID of the alive node with the highest ID (the expected Bully winner).
     *
     * @return expected leader ID, or {@code -1} if all nodes are down
     */
    public int expectedLeaderId() {
        for (int ii = nodes.size() - 1; ii >= 0; ii--) {
            if (nodes.get(ii).isAlive()) {
                return nodes.get(ii).getNodeId();
            }
        }
        return -1;
    }
}


