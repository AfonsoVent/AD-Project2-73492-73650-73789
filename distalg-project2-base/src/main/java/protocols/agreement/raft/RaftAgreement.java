package protocols.agreement.raft;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import protocols.agreement.messages.AppendEntriesMessage;
import protocols.agreement.messages.AppendEntriesReplyMessage;
import protocols.agreement.messages.RequestVoteMessage;
import protocols.agreement.messages.RequestVoteReplyMessage;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.timers.ReconnectTimer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class RaftAgreement extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(RaftAgreement.class);

    public static final short PROTOCOL_ID = 100;
    public static final String PROTOCOL_NAME = "RaftAgreement";
    private Host myself;
    private int votesReceived = 0;
    private List<Host> membership;
    private int joinedInstance = -1;
    private RaftState state;

    public RaftAgreement(Properties props) throws IOException, HandlerRegistrationException  {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1;
        membership = new ArrayList<>();
        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ElectionTimeoutTimer.TIMER_ID, this::uponElectionTimeout);
        registerTimerHandler(HeartbeatTimer.TIMER_ID, this::uponHeartbeatTimeout);


        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        logger.info("Channel {} created, I am {}", cId, myself);
        registerSharedChannel(cId);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, AppendEntriesMessage.MSG_ID, AppendEntriesMessage.serializer);
        registerMessageSerializer(cId, AppendEntriesReplyMessage.MSG_ID, AppendEntriesReplyMessage.serializer);
        registerMessageSerializer(cId, RequestVoteMessage.MSG_ID, RequestVoteMessage.serializer);
        registerMessageSerializer(cId, RequestVoteReplyMessage.MSG_ID, RequestVoteReplyMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, AppendEntriesMessage.MSG_ID, this::uponAppendEntriesMessage, this::uponMsgFail);
            registerMessageHandler(cId, AppendEntriesReplyMessage.MSG_ID, this::uponAppendEntriesReplyMessage, this::uponMsgFail);
            registerMessageHandler(cId, RequestVoteMessage.MSG_ID, this::uponRequestVoteMessage, this::uponMsgFail);
            registerMessageHandler(cId, RequestVoteReplyMessage.MSG_ID, this::uponRequestVoteReplyMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handlers.", e);
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        if (joinedInstance >= 0) return;
        joinedInstance = notification.getJoinInstance();
        membership = notification.getMembership();
        logger.info("Joined instance {}, membership: {}", joinedInstance, membership);
        state = new RaftState();
        setupTimer(new ElectionTimeoutTimer(), randomElectionTimeout());
    }

    private void uponElectionTimeout(ElectionTimeoutTimer timer, short src) {
        if (state.isLeader()) return;
        state.setCurrentTerm(state.getCurrentTerm() + 1);
        state.setRole(CANDIDATE);//mete-se em votacao
        state.setVotedFor(myself);//vota em si mesmo
        triggerNotification(new LeaderChangeNotification(null));
        sendRequestToAll(new RequestVoteMessage(state.getCurrentTerm(), myself, state.getLog().size(), state.getLastLogTerm()));
        setupTimer(new ElectionTimeoutTimer(), randomElectionTimeout());
    }

    private void uponRequestVoteMessage(RequestVoteMessage msg, Host src, short srcProto, int channelId) {
        if (msg.getTerm() < state.getCurrentTerm()) {//se termo for menor do que o current term, nao vota
            sendMessage(src, new RequestVoteReplyMessage(state.getCurrentTerm(), false));
            return;
        }
        if (msg.getTerm() > state.getCurrentTerm()) {//caso contratio, atualiza o term, torna-se follower e apaga vote
            state.setRole(RaftState.Role.FOLLOWER);
            state.setCurrentTerm(msg.getTerm());
            state.setVotedFor(null);
            triggerNotification(new LeaderChangeNotification(null));
        }
        boolean voteGranted = (state.getVotedFor() == null || state.getVotedFor().equals(msg.getCandidateId())) &&
                (msg.getLastLogIndex() >= state.getLog().size() - 1) &&
                (msg.getLastLogTerm() >= state.getLastLogTerm());//vota apenas se for para o mesmo ou se aind an tinha votado, e os logs tem de estar uptodate
        if (voteGranted) {
            state.setVotedFor(msg.getCandidateId());
            setupTimer(new ElectionTimeoutTimer(), randomElectionTimeout());
        }
        sendMessage(src, new RequestVoteReplyMessage(state.getCurrentTerm(), voteGranted));
    }

    private void uponRequestVoteReplyMessage(RequestVoteReplyMessage msg, Host host, short sourceProto, int channelId){
        if (!state.isCandidate() || msg.getTerm() != state.getCurrentTerm()) return;
        if (msg.isVoteGranted()) {
            state.incrementVotesReceived();
            if (state.hasMajorityVotes(membership.size())) {
                becomeLeader();
            }
        } else if (msg.getTerm() > state.getCurrentTerm()) {
            state.setRole(RaftState.Role.FOLLOWER);
            state.setCurrentTerm(msg.getTerm());
            state.setVotedFor(null);
            triggerNotification(new LeaderChangeNotification(null));
        }
    }

    private void uponProposeRequest(ProposeRequest req, short src) {
        if (!state.isLeader()) return;
        state.appendEntry(req.getInstance(), req.getOpId(), req.getOperation());
        replicateToFollowers();
    }

    private void becomeLeader() {
        state.setRole(RaftState.Role.LEADER);
        triggerNotification(new LeaderChangeNotification(myself));
        nextIndex[peer] = lastLogIndex + 1; // Initialize nextIndex for each follower
        matchIndex[peer] = 0;
        setupTimer(new HeartbeatTimer(), heartbeatInterval());
    }

    private void becomeFollower(){
        state.setRole(RaftState.Role.FOLLOWER);
        triggerNotification(new LeaderChangeNotification(null));
        setupTimer(new ElectionTimeoutTimer(), randomElectionTimeout());
    }

    private void applyCommitted() {
        while (state.lastApplied < state.commitIndex) {
            int i = state.lastApplied + 1;
            LogEntry e = state.getLog().get(i);
            triggerNotification(new protocols.agreement.notifications.DecidedNotification(i, e.getOpId(), e.getOperation()));
            state.lastApplied = i;
        }
    }

    private int randomElectionTimeout() {
        return 150 + new Random().nextInt(150);
    }

    private int heartbeatInterval() {
        return 50;
    }

    private void sendRequestToAll(ProtoMessage msg) {
        for (Host h : membership) {
            sendMessage(msg, h);
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed: {}", msg, host, throwable);
    }

    private void incrementVotesReceived() { votesReceived++; }
    private boolean hasMajorityVotes(int n) { return votesReceived > n/2; }
}