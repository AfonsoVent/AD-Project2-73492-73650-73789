package protocols.agreement.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.raft.messages.AppendEntriesMessage;
import protocols.agreement.raft.messages.AppendEntriesReplyMessage;
import protocols.agreement.raft.messages.RequestVoteMessage;
import protocols.agreement.raft.messages.RequestVoteReplyMessage;
import protocols.agreement.raft.timers.ElectionTimeoutTimer;
import protocols.agreement.raft.timers.HeartbeatTimer;
import protocols.agreement.raft.utils.LogEntry;
import protocols.agreement.raft.utils.RaftState;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class RaftAgreement extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(RaftAgreement.class);

    public static final short PROTOCOL_ID = 100;
    public static final String PROTOCOL_NAME = "RaftAgreement";

    private static final int ELECTION_TIMEOUT_MIN_MS = 150;
    private static final int ELECTION_TIMEOUT_RANGE_MS = 150;
    private static final int HEARTBEAT_INTERVAL_MS = 50;

    private Host myself;
    private List<Host> membership;
    private int joinedInstance = -1;
    private RaftState state;
    private int votesReceived;
    private int channelId = -1;
    private long consecutiveFailedElections = 0;
    private long lastFailedElectionTime = 0;

    public RaftAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
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

    @Override
    public void init(Properties props) {
        // Wait for channel and join notifications.
    }

    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        channelId = cId;
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
        if (joinedInstance >= 0) {
            return;
        }
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());
        state = new RaftState();
        state.setCommitIndex(joinedInstance - 1);
        state.setLastApplied(joinedInstance - 1);
        logger.info("Joined instance {}, membership: {}", joinedInstance, membership);
        becomeFollower(state.getCurrentTerm());
    }

    private void uponElectionTimeout(ElectionTimeoutTimer timer, long timerId) {
        if (state == null || state.isLeader()) {
            return;
        }
        
        // Only attempt election if we have peers to contact
        if (peers().isEmpty()) {
            // No peers available, wait longer before trying again
            int waitTime = randomElectionTimeout() * 2;
            setupTimer(new ElectionTimeoutTimer(), waitTime);
            return;
        }
        
        startElection();
    }

    private void startElection() {
        List<Host> peersList = peers();
        
        // If we have no peers, don't start election - wait for peers to join
        if (peersList.isEmpty()) {
            logger.debug("Cannot start election: no peers available. Will retry later.");
            consecutiveFailedElections++;
            resetElectionTimer();
            return;
        }
        
        consecutiveFailedElections = 0;
        state.setCurrentTerm(state.getCurrentTerm() + 1);
        state.setRole(RaftState.ServerRole.CANDIDATE);//mete-se em votacao
        state.setVotedFor(myself);//vota em si mesmo
        votesReceived = 1;
        triggerNotification(new LeaderChangeNotification(null));

        RequestVoteMessage msg = new RequestVoteMessage(
                state.getCurrentTerm(),
                myself,
                state.getLastLogIndex(),
                state.getLastLogTerm());
        sendToPeers(msg);

        if (hasMajorityVotes()) {
            becomeLeader();
            return;
        }
        resetElectionTimer();
    }

    private void uponRequestVoteMessage(RequestVoteMessage msg, Host src, short srcProto, int channelId) {
        if (state == null) {
            return;
        }
        if (msg.getTerm() < state.getCurrentTerm()) {//se termo for menor do que o current term, nao vota
            sendMessage(new RequestVoteReplyMessage(state.getCurrentTerm(), false), src);
            return;
        }
        if (msg.getTerm() > state.getCurrentTerm()) {//caso contratio, atualiza o term, torna-se follower e apaga vote
            becomeFollower(msg.getTerm());
        }

        boolean voteGranted = false;
        if ((state.getVotedFor() == null || state.getVotedFor().equals(msg.getCandidateId()))
                && state.isLogUpToDate(msg.getLastLogIndex(), msg.getLastLogTerm())) {
            state.setVotedFor(msg.getCandidateId());
            voteGranted = true;
            resetElectionTimer();
        }

        sendMessage(new RequestVoteReplyMessage(state.getCurrentTerm(), voteGranted), src);
    }

    private void uponRequestVoteReplyMessage(RequestVoteReplyMessage msg, Host host, short sourceProto, int channelId) {
        if (state == null || !state.isCandidate()) {
            return;
        }

        if (msg.getTerm() > state.getCurrentTerm()) {
            becomeFollower(msg.getTerm());
            return;
        }

        if (msg.getTerm() != state.getCurrentTerm() || !msg.isVoteGranted()) {
            return;
        }

        votesReceived++;
        if (hasMajorityVotes()) {
            becomeLeader();
        }
    }

    private void uponHeartbeatTimeout(HeartbeatTimer timer, long timerId) {
        if (state == null || !state.isLeader()) {
            return;
        }
        
        // Only replicate if we have peers to replicate to
        if (!peers().isEmpty()) {
            replicateToFollowers();
        }
        
        setupTimer(new HeartbeatTimer(), HEARTBEAT_INTERVAL_MS);
    }

    private void uponAppendEntriesMessage(AppendEntriesMessage msg, Host src, short srcProto, int channelId) {
        if (state == null) {
            return;
        }

        if (msg.getTerm() < state.getCurrentTerm()) {
            sendMessage(new AppendEntriesReplyMessage(state.getCurrentTerm(), false, -1), src);
            return;
        }

        if (msg.getTerm() >= state.getCurrentTerm()) {
            if (msg.getTerm() > state.getCurrentTerm()) {
                becomeFollower(msg.getTerm());
            } else {
                state.setRole(RaftState.ServerRole.FOLLOWER);
                state.setVotedFor(null);
            }
            resetElectionTimer();
        }

        boolean success = logMatches(msg.getPrevLogIndex(), msg.getPrevLogTerm());
        if (success) {
            appendEntries(msg.getEntries(), msg.getPrevLogIndex());
            if (msg.getLeaderCommit() > state.getCommitIndex()) {
                int lastNewIndex = msg.getEntries().isEmpty()
                        ? msg.getPrevLogIndex()
                        : msg.getEntries().get(msg.getEntries().size() - 1).getIndex();
                state.setCommitIndex(Math.min(msg.getLeaderCommit(), lastNewIndex));
            }
            applyCommitted();
        }

        int matchIndex = success ? state.getLastLogIndex() : -1;
        sendMessage(new AppendEntriesReplyMessage(state.getCurrentTerm(), success, matchIndex), src);
    }

    private void uponAppendEntriesReplyMessage(AppendEntriesReplyMessage msg, Host host, short sourceProto, int channelId) {
        if (state == null || !state.isLeader()) {
            return;
        }

        if (msg.getTerm() > state.getCurrentTerm()) {
            becomeFollower(msg.getTerm());
            return;
        }

        if (msg.getTerm() != state.getCurrentTerm()) {
            return;
        }

        if (msg.isSuccess()) {
            state.getMatchIndex().put(host, msg.getMatchIndex());
            state.getNextIndex().put(host, msg.getMatchIndex() + 1);
            updateCommitIndex();
            applyCommitted();
        } else {
            int next = state.getNextIndex().getOrDefault(host, state.getLastLogIndex() + 1);
            state.getNextIndex().put(host, Math.max(0, next - 1));
            sendAppendEntries(host);
        }
    }

    private void uponProposeRequest(ProposeRequest req, short src) {
        if (state == null || !state.isLeader()) {
            return;
        }

        int index = req.getInstance();
        if (index <= state.getLastLogIndex()) {
            replicateToFollowers();
            return;
        }

        state.appendEntry(index, state.getCurrentTerm(), req.getOpId(), req.getOperation());
        logger.debug("Leader appended instance {} opId {}", index, req.getOpId());
        replicateToFollowers();
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        if (membership != null && !membership.contains(request.getReplica())) {
            membership.add(request.getReplica());
            logger.info("Added replica {} at instance {}", request.getReplica(), request.getInstance());
        }
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        if (membership != null) {
            membership.remove(request.getReplica());
            state.getNextIndex().remove(request.getReplica());
            state.getMatchIndex().remove(request.getReplica());
            logger.info("Removed replica {} at instance {}", request.getReplica(), request.getInstance());
        }
    }

    private void becomeLeader() {
        state.setRole(RaftState.ServerRole.LEADER);
        triggerNotification(new LeaderChangeNotification(myself));
        state.initializeLeaderState(peers());
        logger.info("Became leader for term {}", state.getCurrentTerm());
        replicateToFollowers();
        setupTimer(new HeartbeatTimer(), HEARTBEAT_INTERVAL_MS);
    }

    private void becomeFollower(int newTerm) {
        state.setRole(RaftState.ServerRole.FOLLOWER);
        state.setCurrentTerm(newTerm);
        state.setVotedFor(null);
        votesReceived = 0;
        triggerNotification(new LeaderChangeNotification(null));
        resetElectionTimer();
    }

    private void replicateToFollowers() {
        List<Host> peersList = peers();
        if (peersList.isEmpty()) {
            logger.debug("No peers available for replication. Skipping replication round.");
            return;
        }
        for (Host peer : peersList) {
            sendAppendEntries(peer);
        }
    }

    private void sendAppendEntries(Host peer) {
        int nextIdx = state.getNextIndex().getOrDefault(peer, state.getLastLogIndex() + 1);
        int prevIdx = nextIdx - 1;
        int prevTerm = 0;
        if (prevIdx >= 0) {
            LogEntry prev = state.getEntryAt(prevIdx);
            if (prev == null) {
                return;
            }
            prevTerm = prev.getTerm();
        }

        List<LogEntry> entries = state.getEntriesFrom(nextIdx);
        AppendEntriesMessage msg = new AppendEntriesMessage(
                state.getCurrentTerm(),
                myself,
                prevIdx,
                prevTerm,
                entries,
                state.getCommitIndex());
        sendMessage(msg, peer);
    }

    private boolean logMatches(int prevLogIndex, int prevLogTerm) {
        if (prevLogIndex < 0) {
            return state.getLog().isEmpty();
        }
        LogEntry entry = state.getEntryAt(prevLogIndex);
        return entry != null && entry.getTerm() == prevLogTerm;
    }

    private void appendEntries(List<LogEntry> entries, int prevLogIndex) {
        int insertIndex = prevLogIndex + 1;
        for (int i = 0; i < entries.size(); i++) {
            int index = insertIndex + i;
            LogEntry incoming = entries.get(i);
            LogEntry existing = state.getEntryAt(index);
            if (existing != null && existing.getTerm() != incoming.getTerm()) {
                state.truncateLogFrom(index);
            }
            if (index >= state.getLog().size()) {
                state.appendEntry(index, incoming.getTerm(), incoming.getOpId(), incoming.getOperation());
            }
        }
    }

    private void updateCommitIndex() {
        for (int n = state.getLastLogIndex(); n > state.getCommitIndex(); n--) {
            int replicas = 1;
            for (Host peer : peers()) {
                if (state.getMatchIndex().getOrDefault(peer, -1) >= n) {
                    replicas++;
                }
            }
            if (replicas > membership.size() / 2) {
                LogEntry entry = state.getEntryAt(n);
                if (entry != null && entry.getTerm() == state.getCurrentTerm()) {
                    state.setCommitIndex(n);
                    break;
                }
            }
        }
    }

    private void applyCommitted() {
        while (state.getLastApplied() < state.getCommitIndex()) {
            int i = state.getLastApplied() + 1;
            LogEntry entry = state.getEntryAt(i);
            if (entry == null) {
                break;
            }
            triggerNotification(new DecidedNotification(i, entry.getOpId(), entry.getOperation()));
            state.setLastApplied(i);
            logger.debug("Applied instance {} opId {}", i, entry.getOpId());
        }
    }

    private List<Host> peers() {
        List<Host> peers = new ArrayList<>();
        if (membership == null) {
            return peers;
        }
        for (Host host : membership) {
            if (!host.equals(myself)) {
                peers.add(host);
            }
        }
        return peers;
    }

    private void sendToPeers(ProtoMessage msg) {
        List<Host> peersList = peers();
        if (peersList.isEmpty()) {
            logger.debug("No peers to send message to. Skipping broadcast.");
            return;
        }
        for (Host peer : peersList) {
            sendMessage(msg, peer);
        }
    }

    private boolean hasMajorityVotes() {
        return membership != null && votesReceived > membership.size() / 2;
    }

    private void resetElectionTimer() {
        setupTimer(new ElectionTimeoutTimer(), randomElectionTimeout());
    }

    private int randomElectionTimeout() {
        return ELECTION_TIMEOUT_MIN_MS + new Random().nextInt(ELECTION_TIMEOUT_RANGE_MS);
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed: {}", msg.getClass().getSimpleName(), host, throwable.getMessage());

        // Check for Connection reset or Native IO issues
        if (throwable instanceof io.netty.channel.unix.Errors.NativeIoException
                || throwable.getMessage().contains("Connection reset")
                || throwable.getMessage().contains("DEAD")) {

            logger.warn("Channel to {} is DEAD or reset. Forcing channel cleanup...", host);

            // Inform your State Machine or Babel's channel manager to close the connection
            // completely before trying to open it again.
            closeConnection(host);

            // Give it a tiny moment or defer the reconnection attempt so you don't spam the OS loop
            openConnection(host);
        }
    }
}
