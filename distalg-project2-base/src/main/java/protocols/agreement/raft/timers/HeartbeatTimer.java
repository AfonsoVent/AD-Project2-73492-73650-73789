package protocols.agreement.raft.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class HeartbeatTimer extends ProtoTimer {

    public static final short TIMER_ID = 2002;

    private final long generation;

    public HeartbeatTimer(long generation) {
        super(TIMER_ID);
        this.generation = generation;
    }

    public long getGeneration() {
        return generation;
    }

    @Override
    public ProtoTimer clone() {
        return new HeartbeatTimer(generation);
    }

    @Override
    public String toString() {
        return "HeartbeatTimer{generation=" + generation + '}';
    }
}
