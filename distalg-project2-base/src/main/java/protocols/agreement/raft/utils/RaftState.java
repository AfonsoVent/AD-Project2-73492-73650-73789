package protocols.agreement.raft.utils;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class RaftState {

    private int currentTerm;
    private Host votedFor;
    private final List<LogEntry> log;
    private static final int SNAPSHOT_THRESHOLD = 10000;
    private int snapshotIndex = -1;
    private int commitIndex;
    private int lastApplied;

    private final Map<Host, Integer> nextIndex;
    private final Map<Host, Integer> matchIndex;

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
        this.commitIndex = -1;
        this.lastApplied = -1;
        this.nextIndex = new HashMap<>();
        this.matchIndex = new HashMap<>();
        this.role = ServerRole.FOLLOWER;
    }

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

    public LogEntry getEntryAt(int index) {
        if (index < 0 || index >= log.size()) {
            return null;
        }
        return log.get(index);
    }

    public int getLastLogIndex() {
        return log.isEmpty() ? -1 : log.size() - 1;
    }

    public int getLastLogTerm() {
        if (log.isEmpty()) {
            return 0;
        }
        return log.get(log.size() - 1).getTerm();
    }

    public boolean isLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        int myLastTerm = getLastLogTerm();
        if (candidateLastLogTerm != myLastTerm) {
            return candidateLastLogTerm > myLastTerm;
        }
        return candidateLastLogIndex >= getLastLogIndex();
    }

    public LogEntry appendEntry(int index, int term, UUID opId, byte[] operation) {
        LogEntry entry = new LogEntry(term, index, opId, operation);
        if (index < log.size()) {
            log.set(index, entry);
        } else {
            log.add(entry);
        }
        return entry;
    }

    public void truncateLogFrom(int fromIndex) {
        if (fromIndex < 0) {
            return;
        }
        while (log.size() > fromIndex) {
            log.remove(log.size() - 1);
        }
    }

    public List<LogEntry> getEntriesFrom(int fromIndex) {
        List<LogEntry> entries = new ArrayList<>();
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        for (int i = fromIndex; i < log.size(); i++) {
            entries.add(log.get(i));
        }
        return entries;
    }

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

    public Map<Host, Integer> getNextIndex() {
        return nextIndex;
    }

    public Map<Host, Integer> getMatchIndex() {
        return matchIndex;
    }

    public void initializeLeaderState(Collection<Host> peers) {
        nextIndex.clear();
        matchIndex.clear();
        int next = getLastLogIndex() + 1;
        for (Host peer : peers) {
            nextIndex.put(peer, next);
            matchIndex.put(peer, -1);
        }
    }

    public int getSnapshotIndex() {
        return snapshotIndex;
    }

    public void setSnapshotIndex(int index) {
        this.snapshotIndex = index;
    }

    public void maybeCompactLog() {
        if (commitIndex - snapshotIndex < SNAPSHOT_THRESHOLD) return;

        int compactTo = commitIndex;

        // remove tudo até esse ponto
        while (!log.isEmpty() && log.get(0).getIndex() <= compactTo) {
            log.remove(0);
        }

        snapshotIndex = compactTo;
    }

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
