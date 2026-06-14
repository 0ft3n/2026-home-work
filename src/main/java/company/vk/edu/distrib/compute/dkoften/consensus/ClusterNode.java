package company.vk.edu.distrib.compute.dkoften.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A single node in a distributed cluster that participates in leader election
 * via the Bully algorithm.
 *
 * <p>Each node runs in its own virtual thread and communicates with peers
 * through a {@link LinkedBlockingQueue} inbox. Nodes may be forcibly failed and
 * recovered to simulate network partitions or hardware faults.
 *
 * <p>Algorithm summary (Bully):
 * <ol>
 *   <li>A node that detects the leader is unavailable sends {@code ELECT} to all peers
 *       with a higher ID.</li>
 *   <li>If no higher-ID peer replies within {@link #ELECTION_TIMEOUT_MS}, the initiating
 *       node declares itself leader and broadcasts {@code VICTORY}.</li>
 *   <li>A peer that receives {@code ELECT} sends {@code ANSWER} and starts its own
 *       election, ensuring the highest alive ID always wins.</li>
 * </ol>
 */
public final class ClusterNode implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterNode.class);

    /** How often (ms) a follower pings the leader to detect failures. */
    static final long PING_INTERVAL_MS = 300L;

    /** How long (ms) to wait for an ANSWER to an ELECT before declaring victory. */
    static final long ELECTION_TIMEOUT_MS = 500L;

    /** How long (ms) without a leader heartbeat before the leader is considered dead. */
    static final long LEADER_DEAD_THRESHOLD_MS = PING_INTERVAL_MS * 3L;

    private final int nodeId;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();

    private final AtomicReference<NodeState> state = new AtomicReference<>(NodeState.FOLLOWER);
    private final AtomicInteger leaderId = new AtomicInteger(-1);
    private final AtomicLong lastLeaderContactMs = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean alive = new AtomicBoolean(true);
    private final AtomicBoolean electionInProgress = new AtomicBoolean();
    private final AtomicReference<CountDownLatch> electionAnswerLatch = new AtomicReference<>();
    private final AtomicReference<List<ClusterNode>> peers = new AtomicReference<>(List.of());
    private final AtomicReference<Thread> nodeThread = new AtomicReference<>();

    /**
     * Creates a new cluster node with the given unique identifier.
     *
     * @param nodeId unique integer ID; higher ID wins elections (Bully invariant)
     */
    public ClusterNode(int nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Sets the peer list from the full cluster node list.
     * Self is automatically excluded. Must be called before {@link #start()}.
     *
     * @param allNodes all cluster nodes including this one
     */
    public void setPeers(List<ClusterNode> allNodes) {
        List<ClusterNode> filtered = new ArrayList<>(allNodes.size());
        for (ClusterNode node : allNodes) {
            if (node.nodeId != this.nodeId) {
                filtered.add(node);
            }
        }
        peers.set(List.copyOf(filtered));
    }

    /**
     * Returns this node's unique ID.
     *
     * @return the node ID
     */
    public int getNodeId() {
        return nodeId;
    }

    /**
     * Returns the current state of this node.
     * Always returns {@link NodeState#DOWN} when the node has failed.
     *
     * @return current {@link NodeState}
     */
    public NodeState getState() {
        return alive.get() ? state.get() : NodeState.DOWN;
    }

    /**
     * Returns the ID of the leader as currently known by this node.
     *
     * @return leader node ID, or {@code -1} if no leader is known
     */
    public int getLeaderId() {
        return leaderId.get();
    }

    /**
     * Returns whether this node is currently operational.
     *
     * @return {@code true} if the node has not failed
     */
    public boolean isAlive() {
        return alive.get();
    }

    /**
     * Delivers a message to this node's inbox.
     * Messages sent to a failed node are silently dropped.
     *
     * @param message the message to deliver
     */
    public void receive(Message message) {
        if (alive.get()) {
            inbox.offer(message);
        }
    }

    /**
     * Simulates a node failure. The node stops processing messages and clears its inbox.
     * Can be reversed with {@link #recover()}.
     */
    public void fail() {
        if (alive.compareAndSet(true, false)) {
            state.set(NodeState.DOWN);
            leaderId.set(-1);
            inbox.clear();
            LOG.info("Node {} FAILED", nodeId);
        }
    }

    /**
     * Simulates node recovery after a failure. The node restarts as a follower and
     * immediately triggers a new election to re-integrate into the cluster.
     */
    public void recover() {
        if (alive.compareAndSet(false, true)) {
            state.set(NodeState.FOLLOWER);
            leaderId.set(-1);
            lastLeaderContactMs.set(0L);
            LOG.info("Node {} RECOVERED - starting election", nodeId);
            triggerElection();
        }
    }

    /**
     * Performs a graceful shutdown. If this node is the leader it broadcasts
     * {@link MessageType#GRACEFUL_DOWN} so peers can elect a new leader immediately
     * rather than waiting for a ping timeout.
     */
    public void gracefulShutdown() {
        if (state.get() == NodeState.LEADER) {
            LOG.info("Node {} (leader) performing graceful shutdown", nodeId);
            broadcastToAll(new Message(MessageType.GRACEFUL_DOWN, nodeId));
        }
        fail();
    }

    /**
     * Starts this node's virtual processing thread and triggers the initial election.
     */
    public void start() {
        running.set(true);
        nodeThread.set(Thread.ofVirtual().name("node-" + nodeId).start(this));
    }

    /**
     * Interrupts and stops this node's processing thread.
     */
    public void stop() {
        running.set(false);
        Thread th = nodeThread.get();
        if (th != null) {
            th.interrupt();
        }
    }

    @Override
    public void run() {
        triggerElection();
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                Message msg = inbox.poll(PING_INTERVAL_MS, TimeUnit.MILLISECONDS);
                if (!alive.get()) {
                    continue;
                }
                if (msg == null) {
                    onTimeout();
                } else {
                    handleMessage(msg);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void onTimeout() {
        if (state.get() != NodeState.FOLLOWER) {
            return;
        }
        int currentLeader = leaderId.get();
        if (currentLeader < 0) {
            triggerElection();
            return;
        }
        long elapsed = System.currentTimeMillis() - lastLeaderContactMs.get();
        if (elapsed > LEADER_DEAD_THRESHOLD_MS) {
            LOG.info("Node {} considers leader {} dead (silent for {} ms)", nodeId, currentLeader, elapsed);
            leaderId.set(-1);
            triggerElection();
        } else {
            sendToNode(currentLeader, new Message(MessageType.PING, nodeId));
        }
    }

    private void handleMessage(Message msg) {
        switch (msg.type()) {
            case PING -> sendToNode(msg.senderId(), new Message(MessageType.ANSWER, nodeId));
            case ELECT -> {
                sendToNode(msg.senderId(), new Message(MessageType.ANSWER, nodeId));
                triggerElection();
            }
            case ANSWER -> {
                if (msg.senderId() == leaderId.get()) {
                    lastLeaderContactMs.set(System.currentTimeMillis());
                }
                countDownElectionLatch();
            }
            case VICTORY -> {
                leaderId.set(msg.senderId());
                state.set(NodeState.FOLLOWER);
                lastLeaderContactMs.set(System.currentTimeMillis());
                LOG.info("Node {} acknowledges node {} as LEADER", nodeId, msg.senderId());
                countDownElectionLatch();
            }
            case GRACEFUL_DOWN -> {
                if (msg.senderId() == leaderId.get()) {
                    leaderId.set(-1);
                    triggerElection();
                }
            }
        }
    }

    private void countDownElectionLatch() {
        CountDownLatch latch = electionAnswerLatch.get();
        if (latch != null) {
            latch.countDown();
        }
    }

    /** Starts a new election round in a dedicated virtual thread, if not already running. */
    private void triggerElection() {
        if (!alive.get()) {
            return;
        }
        if (!electionInProgress.compareAndSet(false, true)) {
            return;
        }
        Thread.ofVirtual().name("elect-" + nodeId).start(() -> {
            try {
                runElection();
            } finally {
                electionInProgress.set(false);
            }
        });
    }

    private void runElection() {
        if (!alive.get()) {
            return;
        }
        LOG.debug("Node {} running election", nodeId);
        List<ClusterNode> higherPeers = collectHigherAlivePeers();
        if (higherPeers.isEmpty()) {
            becomeLeader();
            return;
        }
        CountDownLatch latch = new CountDownLatch(1);
        electionAnswerLatch.set(latch);
        for (ClusterNode peer : higherPeers) {
            peer.receive(new Message(MessageType.ELECT, nodeId));
        }
        try {
            boolean gotAnswer = latch.await(ELECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            electionAnswerLatch.set(null);
            if (!gotAnswer && alive.get() && leaderId.get() < 0) {
                becomeLeader();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            electionAnswerLatch.set(null);
        }
    }

    private List<ClusterNode> collectHigherAlivePeers() {
        List<ClusterNode> result = new ArrayList<>();
        for (ClusterNode peer : peers.get()) {
            if (peer.nodeId > this.nodeId && peer.isAlive()) {
                result.add(peer);
            }
        }
        return result;
    }

    private void becomeLeader() {
        if (!alive.get()) {
            return;
        }
        state.set(NodeState.LEADER);
        leaderId.set(nodeId);
        LOG.info("Node {} declared itself LEADER", nodeId);
        broadcastToAll(new Message(MessageType.VICTORY, nodeId));
    }

    private void sendToNode(int targetId, Message message) {
        for (ClusterNode peer : peers.get()) {
            if (peer.nodeId == targetId) {
                peer.receive(message);
                return;
            }
        }
    }

    private void broadcastToAll(Message message) {
        for (ClusterNode peer : peers.get()) {
            peer.receive(message);
        }
    }

    @Override
    public String toString() {
        return "Node{id=" + nodeId + ", state=" + getState() + ", leader=" + leaderId.get() + "}";
    }
}
