package protocols.agreement.raft.utils;

import java.util.UUID;

/**
 * Represents a single entry in the RAFT log
 */
public class LogEntry {
    
    private final int term;           // Term when entry was received by leader
    private final int index;          // Index in the log
    private final UUID opId;          // Operation ID
    private final byte[] operation;   // Serialized operation/command

    public LogEntry(int term, int index, UUID opId, byte[] operation) {
        this.term = term;
        this.index = index;
        this.opId = opId;
        this.operation = operation;
    }

    public int getTerm() {
        return term;
    }

    public int getIndex() {
        return index;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOperation() {
        return operation;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", index=" + index +
                ", opId=" + opId +
                '}';
    }
}

