package protocols.agreement.multiPaxos.timer;

import java.util.UUID;

import protocols.agreement.multiPaxos.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class AcceptRetryTimer extends ProtoTimer {
    public static final short TIMER_ID = 402;

    private final int instance;
    private final Ballot ballot;
    private final UUID opId;
    private final byte[] value;
    private final int generation;
    private final long delayMs;

    public AcceptRetryTimer(int instance, Ballot ballot, UUID opId, byte[] value, int generation, long delayMs) {
        super(TIMER_ID);
        this.instance = instance;
        this.ballot = ballot;
        this.opId = opId;
        this.value = value;
        this.generation = generation;
        this.delayMs = delayMs;
    }

    public int getInstance() { return instance; }
    public Ballot getBallot() { return ballot; }
    public UUID getOpId() { return opId; }
    public byte[] getValue() { return value; }
    public int getGeneration() { return generation; }
    public long getDelayMs() { return delayMs; }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}