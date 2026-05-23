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
import java.util.concurrent.ThreadLocalRandom;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.requests.StealLeaderRequest;
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

    private static final int ELECTION_TIMEOUT_MIN_MS = 600;
    private static final int ELECTION_TIMEOUT_RANGE_MS = 400;
    private static final int HEARTBEAT_INTERVAL_MS = 50;
    private static final int LOG_COMPACT_KEEP_ENTRIES = 2000;

    private Host myself;
    private List<Host> membership;
    private int joinedInstance = -1;
    private RaftState state;
    private int votesReceived;
    private int channelId = -1;
    private long electionGeneration = 0;
    private long heartbeatGeneration = 0;
    private long electionTimerId = -1;
    private long heartbeatTimerId = -1;
    /** Last leader notified to the state machine (avoids duplicate LeaderChange notifications). */
    private Host stateMachineLeader;

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
        registerRequestHandler(StealLeaderRequest.REQUEST_ID, this::uponStealLeaderRequest);

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
        if (timer.getGeneration() != electionGeneration) {
            return;
        }
        electionTimerId = -1;

        if (peers().isEmpty()) {
            resetElectionTimer();
            return;
        }

        startElection();
    }

    private void startElection() {
        List<Host> peersList = peers();
        
        // If we have no peers, don't start election - wait for peers to join
        if (peersList.isEmpty()) {
            logger.debug("Cannot start election: no peers available. Will retry later.");
            resetElectionTimer();
            return;
        }
        
        state.setCurrentTerm(state.getCurrentTerm() + 1);
        state.setRole(RaftState.ServerRole.CANDIDATE);
        state.setVotedFor(myself);
        votesReceived = 1;
        logger.info("Starting election for term {} (membership size={}, peers={})", state.getCurrentTerm(),
                membership == null ? 0 : membership.size(), peersList);

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
        logger.debug("Received RequestVote from {} for term {} (candidate lastLogIndex={}, lastLogTerm={})",
                src, msg.getTerm(), msg.getLastLogIndex(), msg.getLastLogTerm());
        if (msg.getTerm() < state.getCurrentTerm()) {//se termo for menor do que o current term, nao vota
            sendMessage(new RequestVoteReplyMessage(state.getCurrentTerm(), false), src);
            return;
        }
        if (msg.getTerm() > state.getCurrentTerm()) {//caso contratio, atualiza o term, torna-se follower e apaga vote
            becomeFollower(msg.getTerm());
        }

        boolean voteGranted = false;
        if ((state.getVotedFor() == null || sameHost(state.getVotedFor(), msg.getCandidateId()))
                && state.isLogUpToDate(msg.getLastLogIndex(), msg.getLastLogTerm())) {
            state.setVotedFor(msg.getCandidateId());
            voteGranted = true;
            resetElectionTimer();
            logger.info("Granted vote to {} for term {}", msg.getCandidateId(), msg.getTerm());
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
        logger.info("Received vote reply from {} (term={}, granted={}), votes now {}/{}", host, msg.getTerm(), msg.isVoteGranted(), votesReceived, membership == null ? 0 : membership.size());
        if (hasMajorityVotes()) {
            logger.info("Achieved majority votes ({}/{}) for term {} - becoming leader", votesReceived, membership == null ? 0 : membership.size(), state.getCurrentTerm());
            becomeLeader();
        }
    }

    private void uponHeartbeatTimeout(HeartbeatTimer timer, long timerId) {
        if (state == null || !state.isLeader()) {
            return;
        }
        if (timer.getGeneration() != heartbeatGeneration) {
            return;
        }
        heartbeatTimerId = -1;

        if (!peers().isEmpty()) {
            replicateToFollowers();
        }

        resetHeartbeatTimer();
    }

    private void uponAppendEntriesMessage(AppendEntriesMessage msg, Host src, short srcProto, int channelId) {
        if (state == null) {
            return;
        }
        logger.debug("AppendEntries from {} term={} prevIndex={} prevTerm={} entries={} leaderCommit={}", src, msg.getTerm(), msg.getPrevLogIndex(), msg.getPrevLogTerm(), msg.getEntries() == null ? 0 : msg.getEntries().size(), msg.getLeaderCommit());

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
            notifyLeaderToStateMachine(msg.getLeaderId());
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
        logger.info("Leader appended instance {} opId {}", index, req.getOpId());
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

    private void uponStealLeaderRequest(StealLeaderRequest request, short sourceProto) {
        // Raft elects leaders by vote; ignore steal requests from the state machine.
        logger.debug("Ignoring StealLeaderRequest (Raft uses elections)");
    }

    private void becomeLeader() {
        cancelElectionTimer();
        state.setRole(RaftState.ServerRole.LEADER);
        updateStateMachineLeader(myself);
        state.initializeLeaderState(peers());
        logger.info("Became leader for term {}", state.getCurrentTerm());
        replicateToFollowers();
        resetHeartbeatTimer();
    }

    private void becomeFollower(int newTerm) {
        boolean wasLeader = state.isLeader();
        cancelHeartbeatTimer();
        state.setRole(RaftState.ServerRole.FOLLOWER);
        state.setCurrentTerm(newTerm);
        state.setVotedFor(null);
        votesReceived = 0;
        if (wasLeader) {
            logger.info("Becoming follower for term {} (clearing leader)", newTerm);
            updateStateMachineLeader(null);
        }
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
        // If prevLogIndex < 0 it means there's no previous entry and it should match
        if (prevLogIndex < 0) {
            return true;
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
        state.compactAppliedLog(state.getLastApplied(), LOG_COMPACT_KEEP_ENTRIES);
    }

    private List<Host> peers() {
        List<Host> peers = new ArrayList<>();
        if (membership == null) {
            return peers;
        }
        for (Host host : membership) {
            if (!sameHost(host, myself)) {
                peers.add(host);
            }
        }
        return peers;
    }

    private Host resolveMember(Host host) {
        if (host == null || membership == null) {
            return host;
        }
        for (Host member : membership) {
            if (sameHost(member, host)) {
                return member;
            }
        }
        return host;
    }

    private void notifyLeaderToStateMachine(Host leader) {
        if (leader == null || sameHost(leader, myself)) {
            return;
        }
        updateStateMachineLeader(leader);
    }

    private void updateStateMachineLeader(Host leader) {
        Host resolved = leader == null ? null : resolveMember(leader);
        if (resolved == null && stateMachineLeader == null) {
            return;
        }
        if (resolved != null && stateMachineLeader != null && sameHost(resolved, stateMachineLeader)) {
            return;
        }
        Host previous = stateMachineLeader;
        stateMachineLeader = resolved;
        logger.info("State machine leader: {} -> {}", previous, resolved);
        triggerNotification(new LeaderChangeNotification(resolved));
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
        cancelElectionTimer();
        electionGeneration++;
        electionTimerId = setupTimer(new ElectionTimeoutTimer(electionGeneration), randomElectionTimeout());
    }

    private void resetHeartbeatTimer() {
        cancelHeartbeatTimer();
        heartbeatGeneration++;
        heartbeatTimerId = setupTimer(new HeartbeatTimer(heartbeatGeneration), HEARTBEAT_INTERVAL_MS);
    }

    private void cancelElectionTimer() {
        if (electionTimerId >= 0) {
            cancelTimer(electionTimerId);
            electionTimerId = -1;
        }
        electionGeneration++;
    }

    private void cancelHeartbeatTimer() {
        if (heartbeatTimerId >= 0) {
            cancelTimer(heartbeatTimerId);
            heartbeatTimerId = -1;
        }
        heartbeatGeneration++;
    }

    private boolean sameHost(Host a, Host b) {
        if (a == null || b == null) {
            return false;
        }
        return a.getPort() == b.getPort()
                && a.getAddress().getHostAddress().equals(b.getAddress().getHostAddress());
    }

    private int randomElectionTimeout() {
        return ELECTION_TIMEOUT_MIN_MS + ThreadLocalRandom.current().nextInt(ELECTION_TIMEOUT_RANGE_MS);
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        // Only log at debug level to reduce noise when connections aren't available yet
        if (throwable.getMessage() != null && throwable.getMessage().contains("No outgoing connection")) {
            logger.debug("Cannot send message to {}: no connection available yet", host);
        } else {
            logger.error("Message {} to {} failed: {}", msg, host, throwable);
        }
    }
}
