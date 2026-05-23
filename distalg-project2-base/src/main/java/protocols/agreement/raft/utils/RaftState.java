package protocols.agreement.raft.utils;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class RaftState {

    private int currentTerm;
    private Host votedFor;

    private final List<LogEntry> log;
    private int logStartIndex;

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
        this.logStartIndex = 0;
        this.commitIndex = -1;
        this.lastApplied = -1;
        this.nextIndex = new HashMap<>();
        this.matchIndex = new HashMap<>();
        this.role = ServerRole.FOLLOWER;
    }

    private int toPhysical(int logicalIndex) {
        return logicalIndex - logStartIndex;
    }

    public int getCurrentTerm() { return currentTerm; }
    public void setCurrentTerm(int term) { this.currentTerm = term; }

    public Host getVotedFor() { return votedFor; }
    public void setVotedFor(Host host) { this.votedFor = host; }

    public List<LogEntry> getLog() { return log; }

    public LogEntry getEntryAt(int logicalIndex) {
        if (logicalIndex < 0 || log.isEmpty()) return null;
        int p = toPhysical(logicalIndex);
        if (p < 0 || p >= log.size()) return null;
        return log.get(p);
    }

    public int getLastLogIndex() {
        return log.isEmpty() ? -1 : log.get(log.size() - 1).getIndex();
    }

    public int getLastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
    }

    public boolean isLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        int myLastTerm = getLastLogTerm();
        if (candidateLastLogTerm != myLastTerm) return candidateLastLogTerm > myLastTerm;
        return candidateLastLogIndex >= getLastLogIndex();
    }

    public LogEntry appendEntry(int index, int term, UUID opId, byte[] operation) {
        LogEntry entry = new LogEntry(term, index, opId, operation);
        if (log.isEmpty()) {
            logStartIndex = index;
            log.add(entry);
            return entry;
        }
        int p = toPhysical(index);
        if (p >= 0 && p < log.size()) {
            log.set(p, entry);
        } else if (p == log.size()) {
            log.add(entry);
        } else if (p < 0) {
            return entry;
        } else {
            while (log.size() < p) {
                log.add(null);
            }
            log.add(entry);
        }
        return entry;
    }

    public void truncateLogFrom(int fromLogicalIndex) {
        if (fromLogicalIndex < 0) return;
        int p = toPhysical(fromLogicalIndex);
        if (p < 0) return;
        if (p < log.size()) {
            log.subList(p, log.size()).clear();
        }
    }

    public List<LogEntry> getEntriesFrom(int fromLogicalIndex) {
        int p = toPhysical(fromLogicalIndex);
        if (p < 0) p = 0;
        if (p >= log.size()) return Collections.emptyList();
        return new ArrayList<>(log.subList(p, log.size()));
    }

    public void compactAppliedLog(int lastAppliedIndex, int keepEntries) {
        if (log.isEmpty()) return;
        int removeThrough = lastAppliedIndex - keepEntries;
        if (removeThrough < logStartIndex) return;

        int removeCount = toPhysical(removeThrough) + 1;
        if (removeCount <= 0) return;
        if (removeCount > log.size()) removeCount = log.size();

        log.subList(0, removeCount).clear();
        logStartIndex += removeCount;
    }

    public int getCommitIndex() { return commitIndex; }
    public void setCommitIndex(int index) { this.commitIndex = index; }

    public int getLastApplied() { return lastApplied; }
    public void setLastApplied(int index) { this.lastApplied = index; }

    public Map<Host, Integer> getNextIndex() { return nextIndex; }
    public Map<Host, Integer> getMatchIndex() { return matchIndex; }

    public void initializeLeaderState(Collection<Host> peers) {
        nextIndex.clear();
        matchIndex.clear();
        int next = getLastLogIndex() + 1;
        for (Host peer : peers) {
            nextIndex.put(peer, next);
            matchIndex.put(peer, -1);
        }
    }

    public ServerRole getRole() { return role; }
    public void setRole(ServerRole role) { this.role = role; }

    public boolean isLeader() { return role == ServerRole.LEADER; }
    public boolean isFollower() { return role == ServerRole.FOLLOWER; }
    public boolean isCandidate() { return role == ServerRole.CANDIDATE; }

    @Override
    public String toString() {
        return "RaftState{" +
                "currentTerm=" + currentTerm +
                ", votedFor=" + votedFor +
                ", logSize=" + log.size() +
                ", logStartIndex=" + logStartIndex +
                ", commitIndex=" + commitIndex +
                ", lastApplied=" + lastApplied +
                ", role=" + role +
                '}';
    }
}