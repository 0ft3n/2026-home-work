package company.vk.edu.distrib.compute.dkoften.consensus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Automated integration tests for the Bully leader-election algorithm.
 *
 * <p>Each test scenario starts the cluster, waits for convergence and then
 * exercises a fault-tolerance scenario: leader failure, node recovery or
 * repeated churn. After each action the test waits for consensus and
 * verifies that the Bully invariant holds (highest-alive-ID is the leader).
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class LeaderElectionTest {

    /** Cluster size used in all test cases. */
    private static final int CLUSTER_SIZE = 5;

    /** How long to wait for the cluster to reach consensus (ms). */
    private static final long CONVERGENCE_TIMEOUT_MS = 4_000L;

    /** Polling interval while waiting for consensus (ms). */
    private static final long POLL_INTERVAL_MS = 50L;

    private LeaderElectionCluster cluster;

    @BeforeEach
    void setUp() {
        cluster = new LeaderElectionCluster(CLUSTER_SIZE);
        cluster.start();
    }

    @AfterEach
    void tearDown() {
        cluster.stop();
    }

    // -----------------------------------------------------------------------
    // Scenario 1 – Correct start
    // -----------------------------------------------------------------------

    @Test
    void initialElectionSelectsHighestId() throws InterruptedException {
        awaitConsensus("initial election");

        int leaderId = cluster.getCurrentLeaderId();
        assertEquals(CLUSTER_SIZE, leaderId,
                "Highest-ID node must be elected leader at start-up");
        assertTrue(cluster.hasConsensus(),
                "All alive nodes must agree on the same leader");
    }

    // -----------------------------------------------------------------------
    // Scenario 2 – Leader failure
    // -----------------------------------------------------------------------

    @Test
    void leaderFailureCausesNewElection() throws InterruptedException {
        awaitConsensus("initial election");

        int oldLeader = cluster.getCurrentLeaderId();
        cluster.getNode(oldLeader).fail();

        awaitConsensus("re-election after leader failure");

        int newLeader = cluster.getCurrentLeaderId();
        assertNotEquals(oldLeader, newLeader, "A different node must become leader");
        assertEquals(cluster.expectedLeaderId(), newLeader,
                "New leader must be the alive node with the highest ID");
        assertTrue(cluster.hasConsensus(),
                "All alive nodes must agree on the new leader");
    }

    // -----------------------------------------------------------------------
    // Scenario 3 – Recovered node re-integrates without disrupting consensus
    // -----------------------------------------------------------------------

    @Test
    void recoveredLowerNodeDoesNotDisruptConsensus() throws InterruptedException {
        awaitConsensus("initial election");

        // Fail and recover a non-leader node
        int lowestId = 1;
        cluster.getNode(lowestId).fail();
        Thread.sleep(POLL_INTERVAL_MS * 2);

        cluster.getNode(lowestId).recover();
        awaitConsensus("consensus after node-1 recovery");

        // Current leader is still the highest-alive-ID node (node 5)
        assertEquals(CLUSTER_SIZE, cluster.getCurrentLeaderId(),
                "Original leader must remain after a low-ID node recovers");
        assertTrue(cluster.hasConsensus());
    }

    @Test
    void recoveredHigherNodeBecomesNewLeader() throws InterruptedException {
        awaitConsensus("initial election");

        // Fail the highest node, wait for re-election, then bring it back
        int highestId = CLUSTER_SIZE;
        cluster.getNode(highestId).fail();
        awaitConsensus("re-election without node " + highestId);

        int interimLeader = cluster.getCurrentLeaderId();
        assertNotEquals(highestId, interimLeader);

        // Recover the previously highest node
        cluster.getNode(highestId).recover();
        awaitConsensus("re-election after highest node recovers");

        assertEquals(highestId, cluster.getCurrentLeaderId(),
                "Recovered highest-ID node must reclaim leadership");
        assertTrue(cluster.hasConsensus());
    }

    // -----------------------------------------------------------------------
    // Scenario 4 – Frequent failures and recoveries (stability test)
    // -----------------------------------------------------------------------

    @Test
    void frequentFailuresAndRecoveriesRemainStable() throws InterruptedException {
        awaitConsensus("initial election");

        // Churn through the top three nodes several times
        int rounds = 4;
        for (int round = 0; round < rounds; round++) {
            int target = CLUSTER_SIZE - (round % 3); // cycles 5 → 4 → 3 → 5 …
            cluster.getNode(target).fail();
            Thread.sleep(POLL_INTERVAL_MS * 4);

            cluster.getNode(target).recover();
            awaitConsensus("consensus after churn round " + round);

            assertTrue(cluster.hasConsensus(),
                    "Cluster must reach consensus in churn round " + round);
            assertEquals(cluster.expectedLeaderId(), cluster.getCurrentLeaderId(),
                    "Leader must be the highest alive ID in churn round " + round);
        }
    }

    // -----------------------------------------------------------------------
    // Bonus scenario – Graceful leader shutdown
    // -----------------------------------------------------------------------

    @Test
    void gracefulLeaderShutdownTriggersImmediateReelection() throws InterruptedException {
        awaitConsensus("initial election");

        int oldLeader = cluster.getCurrentLeaderId();
        cluster.getNode(oldLeader).gracefulShutdown();

        awaitConsensus("re-election after graceful leader shutdown");

        int newLeader = cluster.getCurrentLeaderId();
        assertNotEquals(oldLeader, newLeader,
                "A different node must be elected after graceful shutdown");
        assertEquals(cluster.expectedLeaderId(), newLeader,
                "New leader must be the highest alive node");
        assertTrue(cluster.hasConsensus());
    }

    // -----------------------------------------------------------------------
    // Bonus scenario – Cascade failures
    // -----------------------------------------------------------------------

    @Test
    void cascadeLeaderFailuresElectNextHighest() throws InterruptedException {
        awaitConsensus("initial election");

        // Kill nodes from the top down; each time a new node must win
        for (int kill = CLUSTER_SIZE; kill >= 2; kill--) {
            int before = cluster.getCurrentLeaderId();
            cluster.getNode(before).fail();
            awaitConsensus("re-election after killing node " + before);
            assertEquals(cluster.expectedLeaderId(), cluster.getCurrentLeaderId(),
                    "After killing node " + before + " the next-highest must win");
        }
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    /**
     * Blocks until all alive nodes agree on a leader or the timeout expires.
     *
     * @param phase human-readable description used in the timeout error message
     */
    private void awaitConsensus(String phase) throws InterruptedException {
        long deadline = System.currentTimeMillis() + CONVERGENCE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (cluster.hasConsensus()) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        // One last check – produces a descriptive assertion failure if still no consensus
        assertTrue(cluster.hasConsensus(),
                "Cluster did not reach consensus within " + CONVERGENCE_TIMEOUT_MS
                        + " ms during phase: " + phase);
    }
}

