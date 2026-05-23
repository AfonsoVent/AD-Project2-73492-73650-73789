package protocols.agreement;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.messages.AcceptMessage;
import protocols.agreement.messages.AcceptReplyMessage;
import protocols.agreement.messages.DecideMessage;
import protocols.agreement.messages.LeaderAnnouncementMessage;
import protocols.agreement.messages.PrepareMessage;
import protocols.agreement.messages.PrepareReplyMessage;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.requests.StealLeaderRequest;
import protocols.agreement.utils.Ballot;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * Multi-Paxos agreement protocol implementation.
 * 
 * Implements the Multi-Paxos consensus algorithm as specified in the project:
 * - Phase 1 (Prepare): Executed once per leader term to establish supremacy
 * - Phase 2 (Accept): Executed for each new slot in the common case
 * - Supports leader failover with value recovery
 * - Notifies SMR layer of leadership changes and decided values
 */
public class MultiPaxosAgreement extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(MultiPaxosAgreement.class);

    public static final String PROTOCOL_NAME = "MultiPaxosAgreement";
    public static final short PROTOCOL_ID = 101;

    // Configuration
    private Host myself;
    private Set<Host> otherReplicas;
    private int replicaCount;
    private int majoritySize;
    private int channelId;
    
    // Ballot and leadership state
    private Ballot currentBallot;
    private Host currentLeader;
    private boolean isLeader;
    private long ballotCounter;

    // Per-slot Paxos state
    private Map<Integer, SlotState> slotStates;
    private int nextSlotToPropose;
    private int highestDecidedSlot;

    // Pending proposals during leadership changes
    private Map<Integer, PendingProposal> pendingProposals;

    // Phase 1 state
    private Set<Host> prepareOkReplicas;
    private boolean phase1Complete;
    private long maxPreparedBallot;
    
    // Recovered values from Phase 1 (instance -> (ballot, opId, operation))
    private Map<Integer, RecoveredValue> recoveredValues;

    public MultiPaxosAgreement(Properties props) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        slotStates = new HashMap<>();
        pendingProposals = new HashMap<>();
        recoveredValues = new HashMap<>();
        prepareOkReplicas = new HashSet<>();
        otherReplicas = new HashSet<>();
        nextSlotToPropose = 1;
        highestDecidedSlot = 0;
        ballotCounter = 0;
        phase1Complete = false;
        maxPreparedBallot = 0;
        
        // Parse configuration
        try {
            String address = props.getProperty("babel.address");
            int port = Integer.parseInt(props.getProperty("babel.port"));
            myself = new Host(InetAddress.getByName(address), port);
        } catch (UnknownHostException e) {
            logger.error("Failed to parse self address", e);
            throw new IllegalArgumentException("Invalid self address", e);
        }
        
        // Parse replica membership
        String replicasProperty = props.getProperty("initial_membership");
        if (replicasProperty != null && !replicasProperty.isEmpty()) {
            String[] replicas = replicasProperty.split(",");
            replicaCount = replicas.length;
            for (String replica : replicas) {
                try {
                    String[] parts = replica.trim().split(":");
                    if (parts.length == 2) {
                        Host host = new Host(InetAddress.getByName(parts[0].trim()), 
                                            Integer.parseInt(parts[1].trim()));
                        if (!host.equals(myself)) {
                            otherReplicas.add(host);
                        }
                    }
                } catch (UnknownHostException e) {
                    logger.warn("Failed to parse replica: {}", replica, e);
                }
            }
        }
        
        majoritySize = (replicaCount / 2) + 1;
        
        // Initialize ballot state
        currentBallot = new Ballot(0, myself);
        currentLeader = myself;
        isLeader = false; // Will become leader after Phase 1
        
        logger.info("MultiPaxosAgreement initialized - myself: {}, replicas: {}, majority: {}",
                    myself, replicaCount, majoritySize);
        
        // Setup TCP channel for inter-replica communication
        setupTCPChannel();
        
        // Register request handlers
        try {
            registerRequestHandler(ProposeRequest.REQUEST_ID, this::handleProposeRequest);
            registerRequestHandler(StealLeaderRequest.REQUEST_ID, this::handleStealLeaderRequest);
            registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::handleAddReplicaRequest);
            registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::handleRemoveReplicaRequest);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering request handlers", e);
            throw e;
        }
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException {
        // Initialization now done in constructor
    }

    private void setupTCPChannel() throws HandlerRegistrationException, IOException {
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, myself.getAddress().getHostAddress());
        channelProps.setProperty(TCPChannel.PORT_KEY, String.valueOf(myself.getPort()));
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        
        channelId = createChannel(TCPChannel.NAME, channelProps);
        
        // Register channel event handlers
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        
        // Register message serializers and handlers
        registerMessageSerializer(channelId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageHandler(channelId, PrepareMessage.MSG_ID, this::handlePrepareMessage, this::uponMessageFail);
        
        registerMessageSerializer(channelId, PrepareReplyMessage.MSG_ID, PrepareReplyMessage.serializer);
        registerMessageHandler(channelId, PrepareReplyMessage.MSG_ID, this::handlePrepareReplyMessage, this::uponMessageFail);
        
        registerMessageSerializer(channelId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageHandler(channelId, AcceptMessage.MSG_ID, this::handleAcceptMessage, this::uponMessageFail);
        
        registerMessageSerializer(channelId, AcceptReplyMessage.MSG_ID, AcceptReplyMessage.serializer);
        registerMessageHandler(channelId, AcceptReplyMessage.MSG_ID, this::handleAcceptReplyMessage, this::uponMessageFail);
        
        registerMessageSerializer(channelId, DecideMessage.MSG_ID, DecideMessage.serializer);
        registerMessageHandler(channelId, DecideMessage.MSG_ID, this::handleDecideMessage, this::uponMessageFail);
        
        registerMessageSerializer(channelId, LeaderAnnouncementMessage.MSG_ID, LeaderAnnouncementMessage.serializer);
        registerMessageHandler(channelId, LeaderAnnouncementMessage.MSG_ID, this::handleLeaderAnnouncementMessage, this::uponMessageFail);
    }

    // ===== Channel Event Handlers =====

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down", event.getNode());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<?> event, int channelId) {
        logger.debug("Connection to {} failed", event.getNode());
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.debug("Connection to {} is up", event.getNode());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.debug("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.debug("Connection from {} is down", event.getNode());
    }

    private void uponMessageFail(Object msg, Host host, short destProto, Throwable cause, int channelId) {
        logger.debug("Message {} to {} failed: {}", msg.getClass().getSimpleName(), host, cause.getMessage());
    }

    // ===== Request Handlers =====

    private void handleProposeRequest(ProposeRequest request, short sourceProto) {
        int instance = request.getInstance();
        byte[] operation = request.getOperation();
        UUID opId = request.getOpId();
        
        logger.debug("Received ProposeRequest for instance {}: {}", instance, 
                     Hex.encodeHexString(operation).substring(0, Math.min(16, Hex.encodeHexString(operation).length())));
        
        if (isLeader) {
            // Leader proposes directly to Phase 2
            acceptProposal(instance, opId, operation);
        } else {
            // Non-leader stores pending proposal for later resubmission
            pendingProposals.put(instance, new PendingProposal(opId, operation));
            logger.debug("Stored pending proposal for instance {} (not leader)", instance);
        }
    }

    private void handleStealLeaderRequest(StealLeaderRequest request, short sourceProto) {
        logger.info("Received StealLeaderRequest - attempting to become leader");
        // Initiate Phase 1 with a higher ballot to become leader
        incrementBallot();
        beginPhase1();
    }

    private void handleAddReplicaRequest(AddReplicaRequest request, short sourceProto) {
        logger.info("AddReplicaRequest received (not implemented in base version)");
    }

    private void handleRemoveReplicaRequest(RemoveReplicaRequest request, short sourceProto) {
        logger.info("RemoveReplicaRequest received (not implemented in base version)");
    }

    // ===== Message Handlers =====

    private void handlePrepareMessage(PrepareMessage msg, Host from, short sourceProto, int channelId) {
        Ballot ballot = msg.getBallot();
        logger.debug("Received Prepare({}) from {}", ballot, from);
        
        // Update max prepared ballot if higher
        boolean accept = (ballot.compareTo(currentBallot) > 0);
        
        if (accept) {
            currentBallot = ballot;
            maxPreparedBallot = ballot.getRound();
            logger.debug("Updated currentBallot to {}", ballot);
        }
        
        // Send PrepareReplyMessage with accepted values
        List<PrepareReplyMessage.AcceptedValue> acceptedValues = new ArrayList<>(getAcceptedValues().values());
        PrepareReplyMessage reply = new PrepareReplyMessage(ballot, accept, acceptedValues);
        sendMessage(from, reply);
    }

    private void handlePrepareReplyMessage(PrepareReplyMessage msg, Host from, short sourceProto, int channelId) {
        if (!msg.isOk() || msg.getBallot().compareTo(currentBallot) != 0) {
            return;
        }
        
        logger.debug("Received PrepareReply from {} for ballot {}", from, msg.getBallot());
        prepareOkReplicas.add(from);
        
        // Store recovered values from this acceptor
        if (msg.getAcceptedValues() != null) {
            for (PrepareReplyMessage.AcceptedValue acceptedVal : msg.getAcceptedValues()) {
                int instance = acceptedVal.getInstance();
                Ballot aBallot = acceptedVal.getBallot();
                
                // Keep value with highest ballot for each instance
                RecoveredValue existing = recoveredValues.get(instance);
                if (existing == null || aBallot.compareTo(existing.ballot) > 0) {
                    recoveredValues.put(instance, new RecoveredValue(aBallot, acceptedVal.getOpId(), acceptedVal.getOperation()));
                }
            }
        }
        
        if (prepareOkReplicas.size() >= majoritySize && !phase1Complete) {
            phase1Complete = true;
            isLeader = true;
            logger.info("Phase 1 complete - became leader with ballot {}. Recovered values from majority.", currentBallot);
            
            // Send LeaderAnnouncement to all replicas
            broadcastLeaderAnnouncement();
            
            // Notify SMR layer
            triggerNotification(new LeaderChangeNotification(myself));
            
            // Resubmit any pending proposals (now with recovered values)
            resubmitPendingProposals();
        }
    }

    private void handleAcceptMessage(AcceptMessage msg, Host from, short sourceProto, int channelId) {
        Ballot ballot = msg.getBallot();
        int instance = msg.getInstance();
        UUID opId = msg.getOpId();
        byte[] operation = msg.getOperation();
        
        logger.debug("Received Accept({}, {}, ...) from {}", ballot, instance, from);
        
        SlotState slot = getOrCreateSlot(instance);
        
        // Only accept if ballot >= currentBallot for this instance
        boolean accept = (slot.acceptedBallot == null || ballot.compareTo(slot.acceptedBallot) >= 0);
        
        if (accept) {
            slot.acceptedBallot = ballot;
            slot.acceptedOpId = opId;
            slot.acceptedOp = operation;
            logger.debug("Accepted value for instance {}", instance);
        }
        
        // Send AcceptReplyMessage
        AcceptReplyMessage reply = new AcceptReplyMessage(ballot, instance, accept);
        sendMessage(from, reply);
    }

    private void handleAcceptReplyMessage(AcceptReplyMessage msg, Host from, short sourceProto, int channelId) {
        if (!isLeader) {
            return;
        }
        
        int instance = msg.getInstance();
        Ballot ballot = msg.getBallot();
        logger.debug("Received AcceptReply for instance {} with ballot {} from {}", instance, ballot, from);
        
        SlotState slot = getOrCreateSlot(instance);
        
        // LEARNER RULE: Handle ballot updates
        if (slot.phase2Ballot == null) {
            slot.phase2Ballot = ballot;
        }
        
        if (ballot.compareTo(slot.phase2Ballot) > 0) {
            // Higher ballot seen - reset vote count (learner rule: aset.reset())
            slot.phase2Ballot = ballot;
            slot.acceptReplies.clear();
            logger.debug("Higher ballot {} seen for instance {}. Resetting accept votes.", ballot, instance);
        }
        
        // Only count votes for current phase2Ballot
        if (ballot.compareTo(slot.phase2Ballot) != 0) {
            logger.debug("Ignoring AcceptReply with ballot {} for instance {} (current ballot: {})", 
                        ballot, instance, slot.phase2Ballot);
            return;
        }
        
        if (msg.isOk()) {
            slot.acceptReplies.add(from);
        }
        
        // If majority accepted, decide the value
        if (slot.acceptReplies.size() >= majoritySize && !slot.isDecided) {
            slot.isDecided = true;
            highestDecidedSlot = Math.max(highestDecidedSlot, instance);
            logger.info("Instance {} decided with ballot {} and opId: {}", instance, ballot, slot.acceptedOpId);
            
            // Broadcast decide message
            broadcastDecideMessage(slot.phase2Ballot, instance, slot.acceptedOpId, slot.acceptedOp);
            
            // Notify SMR layer
            triggerNotification(new DecidedNotification(instance, slot.acceptedOpId, slot.acceptedOp));
        }
    }

    private void handleDecideMessage(DecideMessage msg, Host from, short sourceProto, int channelId) {
        int instance = msg.getInstance();
        UUID opId = msg.getOpId();
        byte[] operation = msg.getOperation();
        
        logger.debug("Received Decide({}, ...) from {}", instance, from);
        
        SlotState slot = getOrCreateSlot(instance);
        if (!slot.isDecided) {
            slot.isDecided = true;
            slot.acceptedOpId = opId;
            slot.acceptedOp = operation;
            highestDecidedSlot = Math.max(highestDecidedSlot, instance);
            
            // Notify SMR layer
            triggerNotification(new DecidedNotification(instance, opId, operation));
        }
    }

    private void handleLeaderAnnouncementMessage(LeaderAnnouncementMessage msg, Host from, short sourceProto, int channelId) {
        Ballot ballot = msg.getBallot();
        Host newLeader = ballot.getProposer();
        logger.info("Received LeaderAnnouncement: new leader is {} with ballot {}", newLeader, ballot);
        
        if (!newLeader.equals(currentLeader)) {
            currentLeader = newLeader;
            isLeader = currentLeader.equals(myself);
            
            // Notify SMR layer of leadership change
            triggerNotification(new LeaderChangeNotification(newLeader));
        }
    }

    // ===== Phase 1: Prepare =====

    private void beginPhase1() {
        logger.info("Beginning Phase 1 with ballot {}", currentBallot);
        phase1Complete = false;
        prepareOkReplicas.clear();
        prepareOkReplicas.add(myself); // Count self
        recoveredValues.clear(); // Clear old recovered values for new ballot
        
        // Send Prepare message to all other replicas
        PrepareMessage msg = new PrepareMessage(currentBallot);
        for (Host replica : otherReplicas) {
            sendMessage(replica, msg);
        }
    }

    // ===== Phase 2: Accept =====

    private void acceptProposal(int instance, UUID opId, byte[] operation) {
        SlotState slot = getOrCreateSlot(instance);
        
        if (!phase1Complete) {
            // Need to complete Phase 1 first
            beginPhase1();
            // Store the proposal to resubmit after Phase 1
            pendingProposals.put(instance, new PendingProposal(opId, operation));
            return;
        }
        
        // PROPOSER RULE: Use recovered value if exists, otherwise use proposed value
        UUID finalOpId = opId;
        byte[] finalOperation = operation;
        if (recoveredValues.containsKey(instance)) {
            RecoveredValue recovered = recoveredValues.get(instance);
            finalOpId = recovered.opId;
            finalOperation = recovered.operation;
            logger.debug("Using recovered value for instance {}: opId={}", instance, finalOpId);
        } else {
            logger.debug("Using proposed value for instance {}: opId={}", instance, finalOpId);
        }
        
        slot.phase2Ballot = currentBallot;
        slot.acceptedBallot = currentBallot;
        slot.acceptedOpId = finalOpId;
        slot.acceptedOp = finalOperation;
        slot.acceptReplies.clear();
        slot.acceptReplies.add(myself); // Count self
        
        logger.debug("Leader proposing for instance {}: opId={}, ballot={}", instance, finalOpId, currentBallot);
        
        // Send Accept message to all other replicas
        AcceptMessage msg = new AcceptMessage(currentBallot, instance, finalOpId, finalOperation);
        for (Host replica : otherReplicas) {
            sendMessage(replica, msg);
        }
    }

    // ===== Decide =====

    private void broadcastDecideMessage(Ballot ballot, int instance, UUID opId, byte[] operation) {
        DecideMessage msg = new DecideMessage(ballot, instance, opId, operation);
        
        for (Host replica : otherReplicas) {
            sendMessage(replica, msg);
        }
    }

    // ===== Leader Announcement =====

    private void broadcastLeaderAnnouncement() {
        LeaderAnnouncementMessage msg = new LeaderAnnouncementMessage(currentBallot);
        
        for (Host replica : otherReplicas) {
            sendMessage(replica, msg);
        }
    }

    // ===== Utilities =====

    private void incrementBallot() {
        ballotCounter++;
        currentBallot = new Ballot(ballotCounter, myself);
    }

    private void resubmitPendingProposals() {
        List<Integer> instancesToRemove = new ArrayList<>();
        
        for (Map.Entry<Integer, PendingProposal> entry : pendingProposals.entrySet()) {
            int instance = entry.getKey();
            PendingProposal proposal = entry.getValue();
            
            acceptProposal(instance, proposal.opId, proposal.operation);
            instancesToRemove.add(instance);
        }
        
        for (int instance : instancesToRemove) {
            pendingProposals.remove(instance);
        }
    }

    private Map<Integer, PrepareReplyMessage.AcceptedValue> getAcceptedValues() {
        Map<Integer, PrepareReplyMessage.AcceptedValue> values = new HashMap<>();
        
        for (Map.Entry<Integer, SlotState> entry : slotStates.entrySet()) {
            SlotState slot = entry.getValue();
            if (slot.acceptedBallot != null) {
                values.put(entry.getKey(), 
                          new PrepareReplyMessage.AcceptedValue(
                              entry.getKey(),
                              slot.acceptedBallot,
                              slot.acceptedOpId,
                              slot.acceptedOp));
            }
        }
        
        return values;
    }

    private SlotState getOrCreateSlot(int instance) {
        return slotStates.computeIfAbsent(instance, k -> new SlotState());
    }

    private <T extends ProtoMessage> void sendMessage(Host target, T msg) {
        logger.debug("Sending {} to {}", msg.getClass().getSimpleName(), target);
        super.sendMessage(msg, target);
    }

    // ===== Inner Classes =====

    private static class SlotState {
        Ballot acceptedBallot;  // Highest ballot this acceptor has accepted
        UUID acceptedOpId;
        byte[] acceptedOp;
        Ballot phase2Ballot;    // Ballot we're currently in Phase 2 for (proposer's ballot)
        Set<Host> acceptReplies; // Acceptors that replied OK for phase2Ballot
        boolean isDecided;

        SlotState() {
            this.acceptReplies = new HashSet<>();
            this.isDecided = false;
        }
    }

    private static class PendingProposal {
        UUID opId;
        byte[] operation;

        PendingProposal(UUID opId, byte[] operation) {
            this.opId = opId;
            this.operation = operation;
        }
    }

    private static class RecoveredValue {
        Ballot ballot;
        UUID opId;
        byte[] operation;

        RecoveredValue(Ballot ballot, UUID opId, byte[] operation) {
            this.ballot = ballot;
            this.opId = opId;
            this.operation = operation;
        }
    }
}
