package company.vk.edu.distrib.compute.dkoften.consensus;

/**
 * Defines the types of messages exchanged between cluster nodes
 * during leader election (Bully algorithm).
 */
public enum MessageType {
    /**
     * Sent periodically from follower nodes to the leader
     * to verify that the leader is still available.
     */
    PING,

    /**
     * Sent by a node to all peers with a higher ID
     * to initiate an election round.
     */
    ELECT,

    /**
     * Acknowledgement sent in response to a {@link #PING}
     * or {@link #ELECT} message.
     */
    ANSWER,

    /**
     * Broadcast by the newly elected leader to all other nodes
     * to announce its leadership.
     */
    VICTORY,

    /**
     * Sent by the current leader when shutting down gracefully,
     * allowing peers to start a new election without waiting for a timeout.
     */
    GRACEFUL_DOWN
}

