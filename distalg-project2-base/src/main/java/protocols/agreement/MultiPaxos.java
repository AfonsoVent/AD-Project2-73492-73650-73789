package protocols.agreement;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.messages.BroadcastMessage;
import protocols.agreement.messages.PrepareMessage;
import protocols.agreement.messages.PrepareOKMessage;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.utils.Ballot;
import protocols.agreement.utils.PaxosSlot;
import protocols.agreement.utils.QuorumUtils;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * This is NOT a correct agreement protocol (it is actually a VERY wrong one)
 * This is simply an example of things you can do, and can be used as a starting point.
 *
 * You are free to change/delete ANYTHING in this class, including its fields.
 * Do not assume that any logic implemented here is correct, think for yourself!
 */
public class MultiPaxos extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(MultiPaxos.class);

    // Ballot
    private UUID ballotOwner;
    private Ballot promisedBallot;

    private Ballot currentBallot; // Current number ballot
    private Host currentLeader;// Who is the leader
    private boolean amILeader; // I'm the leader?

    // Phase 1
    private Set<Host> phase1Promises; // Who promised in this ballot
    private int majority;             // Size of the majority
    private Queue<ProposeRequest> pendingProposals;

    // Register of the slots
    private Map<Integer, PaxosSlot> slots;

    // Protocol information
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "MultiPaxos";

    // Information channel
    private Host myself;
    private int joinedInstance;
    private List<Host> membership;

    public MultiPaxos(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        ballotOwner = UUID.randomUUID();
        pendingProposals = new LinkedList<>();

        promisedBallot = null;
        membership = null;

        /*--------------------- Register Timer Handlers ----------------------------- */

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {}

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        
        // Debug
        logger.info("Channel {} created, I am {}", cId, myself);
        
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(cId, PrepareOKMessage.MSG_ID, PrepareOKMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareOKMessage.MSG_ID, this::uponPrepareOKMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }
    }

    private void uponPrepareMessage(PrepareMessage msg, Host from, short sourceProto, int channelId) {
        Ballot incoming = msg.getBallot();
        logger.debug("Received Prepare {} from {}", incoming, from);

        if (promisedBallot != null && incoming.compareTo(promisedBallot) <= 0) {
            logger.debug("Ignoring Prepare {} from {} because promisedBallot={}", incoming, from, promisedBallot);
            return;
        }

        promisedBallot = incoming;

        Map<Integer, PrepareOKMessage.SlotStateData> acceptedSlots = new HashMap<>();
        for (Map.Entry<Integer, PaxosSlot> entry : slots.entrySet()) {
            PaxosSlot slot = entry.getValue();

            if (slot.getHighestAcceptSeen() != null && slot.getAcceptedValue() != null) {
                acceptedSlots.put(entry.getKey(),
                        new PrepareOKMessage.SlotStateData(slot.getHighestAcceptSeen(), slot.getAcceptedValue()));
            }
        }

        sendMessage(new PrepareOKMessage(incoming, acceptedSlots), from);

        logger.debug("Sent PrepareOK {} -> {} (acceptedSlots={})", incoming, from, acceptedSlots.size());
    }

    private void uponPrepareOKMessage(PrepareOKMessage msg, Host from, short sourceProto, int channelId) {
        Ballot replyBallot = msg.getBallot();
        logger.debug("Received PrepareOK {} from {}", replyBallot, from);

        // Ignore older Ballots or Ballots from previous leadership attempts
        if (currentBallot == null || !replyBallot.equals(currentBallot)) {
            logger.debug("Ignoring PrepareOK {} from {} because currentBallot={}", replyBallot, from, currentBallot);
            return;
        }

        // Is the leader
        if (amILeader) return;

        // If necessary starts phase1
        if (phase1Promises == null) phase1Promises = new HashSet<>();

        // Register phase1 promise, ignore duplicates
        if (!phase1Promises.add(from)) return;

        logger.debug("Phase1 ballot {} promises: {}/{}", currentBallot, phase1Promises.size(), majority);

        int quorum = QuorumUtils.majority(membership.size());
        logger.debug("Phase1 ballot {} promises: {}/{}", currentBallot, phase1Promises.size(), quorum);

        if (!QuorumUtils.hasMajority(phase1Promises.size(), membership.size())) return;

        amILeader = true;
        currentLeader = myself;
        promisedBallot = currentBallot;

        logger.info("Became leader with ballot {}", currentBallot);
        
        for (Map.Entry<Integer, PrepareOKMessage.SlotStateData> entry : msg.getAcceptedSlots().entrySet()) {
            int instance = entry.getKey();
            PrepareOKMessage.SlotStateData slotData = entry.getValue();

            PaxosSlot slot = slots.computeIfAbsent(instance, k -> new PaxosSlot());

            if (slotData.highestAcceptSeen != null) {
                Ballot localAccept = slot.getHighestAcceptSeen();
                if (localAccept == null || slotData.highestAcceptSeen.compareTo(localAccept) > 0) {
                    slot.setHighestAcceptSeen(slotData.highestAcceptSeen);
                    slot.setAcceptedValue(slotData.value);
                }
            }
        }
    }

    private void startPhase1() {
        if (membership == null || membership.isEmpty()) {
            logger.debug("Cannot start Phase 1 without membership");
            return;
        }

        if (phase1Promises == null) {
            phase1Promises = new HashSet<>();
        } else {
            phase1Promises.clear();
        }

        amILeader = false;
        currentLeader = null;

        if (currentBallot == null) {
            currentBallot = Ballot.initial(ballotOwner);
        } else {
            currentBallot = currentBallot.next();
        }

        logger.info("Starting Phase 1 with ballot {}", currentBallot);

        PrepareMessage prepare = new PrepareMessage(currentBallot);
        membership.forEach(host -> sendMessage(prepare, host));
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());

        if (slots == null) {
            slots = new HashMap<>();
        }

        if (phase1Promises == null) {
            phase1Promises = new HashSet<>();
        }

        majority = QuorumUtils.majority(membership.size());

        logger.info("Agreement starting at instance {}, membership: {}, majority: {}",
                joinedInstance, membership, majority);

        startPhase1();
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("Received {}", request);

        if (joinedInstance < 0) {
            logger.debug("Still joining, buffering proposal {}", request.getOpId());
            pendingProposals.add(request);
            return;
        }

        if (!amILeader) {
            logger.debug("Not leader yet, buffering proposal {} and starting Phase 1 if needed", request.getOpId());
            pendingProposals.add(request);

            if (currentBallot == null) startPhase1();
            return;
        }

        // TODO: Phase 2 ficará aqui depois
        logger.debug("I am leader, proposal {} can proceed to Accept phase", request.getOpId());
    }







    private void uponBroadcastMessage(BroadcastMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            //Obviously your agreement protocols will not decide things as soon as you receive the first message
            triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
        } else {
            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
            //agreement instances, maybe we should do something with them...?
        }
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The AddReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.add(request.getReplica());
    }
    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The RemoveReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.remove(request.getReplica());
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
