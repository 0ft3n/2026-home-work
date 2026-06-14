package company.vk.edu.distrib.compute.dkoften.consensus;

/**
 * Represents the current operational state of a cluster node.
 */
public enum NodeState {

    /** The node is the elected leader and coordinates the cluster. */
    LEADER,

    /** The node is operational and follows the current leader. */
    FOLLOWER,

    /** The node has failed and is not participating in the cluster. */
    DOWN
}

