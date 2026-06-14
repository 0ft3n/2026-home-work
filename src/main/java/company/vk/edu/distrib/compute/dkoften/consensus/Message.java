package company.vk.edu.distrib.compute.dkoften.consensus;

/**
 * An immutable message exchanged between cluster nodes during leader election.
 *
 * @param type the message type
 * @param senderId the unique ID of the node that sent this message
 */
public record Message(MessageType type, int senderId) {
}

