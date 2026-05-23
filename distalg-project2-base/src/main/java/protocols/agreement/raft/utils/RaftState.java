package protocols.agreement.raft.utils;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class RaftState {

    private int currentTerm;
    private Host votedFor;
    private final ArrayDeque<LogEntry> log;
    private int logStartIndex = 0;

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

    private int firstLogIndex() {
        return log.isEmpty() ? 0 : log.get(0).getIndex();
    }

    public LogEntry getEntryAt(int index) {
        if (index < 0 || log.isEmpty()) {
            return null;
        }
        int offset = index - firstLogIndex();
        if (offset < 0 || offset >= log.size()) {
            return null;
        }
        return log.get(offset);
    }

    public int getLastLogIndex() {
        return log.isEmpty() ? -1 : log.get(log.size() - 1).getIndex();
    }

    public int getLastLogTerm() {
        if (log.isEmpty()) {
            return 0;
        }
        return log.get(log.size() - 1).getTerm();
    }

    /** Drop applied entries from memory; keep a tail for replication. */
    public void compactAppliedLog(int lastAppliedIndex, int keepEntries) {
        if (log.isEmpty()) return;
        int removeThrough = lastAppliedIndex - keepEntries;
        while (!log.isEmpty() && log.peekFirst().getIndex() <= removeThrough) {
            log.pollFirst();
            logStartIndex++;
        }
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
        if (log.isEmpty()) {
            log.add(entry);
            return entry;
        }
        int offset = index - firstLogIndex();
        if (offset >= 0 && offset < log.size()) {
            log.set(offset, entry);
        } else if (offset == log.size()) {
            log.add(entry);
        } else {
            log.add(entry);
        }
        return entry;
    }

    public void truncateLogFrom(int fromIndex) {
        if (fromIndex < 0) {
            return;
        }
        while (!log.isEmpty() && log.get(log.size() - 1).getIndex() >= fromIndex) {
            log.remove(log.size() - 1);
        }
    }

    public List<LogEntry> getEntriesFrom(int fromIndex) {
        List<LogEntry> entries = new ArrayList<>();
        int offset = fromIndex - logStartIndex;
        if (offset < 0) offset = 0;
        LogEntry[] arr = log.toArray(new LogEntry[0]);
        for (int i = offset; i < arr.length; i++) {
            entries.add(arr[i]);
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
