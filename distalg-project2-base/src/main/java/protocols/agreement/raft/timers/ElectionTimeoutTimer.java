package protocols.agreement.raft.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ElectionTimeoutTimer extends ProtoTimer {

    public static final short TIMER_ID = 2001;

    private final long generation;

    public ElectionTimeoutTimer(long generation) {
        super(TIMER_ID);
        this.generation = generation;
    }

    public long getGeneration() {
        return generation;
    }

    @Override
    public ProtoTimer clone() {
        return new ElectionTimeoutTimer(generation);
    }

    @Override
    public String toString() {
        return "ElectionTimeoutTimer{generation=" + generation + '}';
    }
}
