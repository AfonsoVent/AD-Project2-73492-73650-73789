package protocols.agreement.raft.utils;

import pt.unl.fct.di.novasys.network.data.Host;
import java.util.*;

/**
 * Persistent RAFT state
 * Should be stored on disk for recovery
 */
public class RaftState {

    // Persistent state (on all servers)
    private int currentTerm;           // Latest term server has seen
    private Host votedFor;             // Candidate received vote in current term
    private List<LogEntry> log;        // Log entries

    // Volatile state (on all servers)
    private int commitIndex;           // Highest log index known to be committed
    private int lastApplied;           // Highest log index applied to state machine

    // Volatile state (on leaders only)
    private Map<Host, Integer> nextIndex;  // Next log index to send to each server
    private Map<Host, Integer> matchIndex; // Highest log index known to be replicated on server

    // Server role
    public enum ServerRole {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }
    private ServerRole role;

    public RaftState() {
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = new ArrayList<>();
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new HashMap<>();
        this.matchIndex = new HashMap<>();
        this.role = ServerRole.FOLLOWER;
    }

    // Getters and setters for persistent state
    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int term) {
        this.currentTerm = term;
    }

    public Host getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(Host host) {
        this.votedFor = host;
    }

    public List<LogEntry> getLog() {
        return log;
    }

    public void addLogEntry(LogEntry entry) {
        this.log.add(entry);
    }

    // Getters and setters for volatile state
    public int getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(int index) {
        this.commitIndex = index;
    }

    public int getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(int index) {
        this.lastApplied = index;
    }

    // Leader state
    public Map<Host, Integer> getNextIndex() {
        return nextIndex;
    }

    public void initializeLeaderState(List<Host> peers) {
        int logSize = log.isEmpty() ? 1 : log.size() + 1;
        for (Host peer : peers) {
            nextIndex.put(peer, logSize);
            matchIndex.put(peer, 0);
        }
    }

    public Map<Host, Integer> getMatchIndex() {
        return matchIndex;
    }

    // Role management
    public ServerRole getRole() {
        return role;
    }

    public void setRole(ServerRole role) {
        this.role = role;
    }

    public boolean isLeader() {
        return role == ServerRole.LEADER;
    }

    public boolean isFollower() {
        return role == ServerRole.FOLLOWER;
    }

    public boolean isCandidate() {
        return role == ServerRole.CANDIDATE;
    }

    @Override
    public String toString() {
        return "RaftState{" +
                "currentTerm=" + currentTerm +
                ", votedFor=" + votedFor +
                ", logSize=" + log.size() +
                ", commitIndex=" + commitIndex +
                ", lastApplied=" + lastApplied +
                ", role=" + role +
                '}';
    }
}

