package protocols.agreement.raft.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Election Timeout Timer
 * Used to trigger leader election when no heartbeat is received from the leader
 */
public class ElectionTimeoutTimer extends ProtoTimer {

    public static final short TIMER_ID = 2001;

    public ElectionTimeoutTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

    @Override
    public String toString() {
        return "ElectionTimeoutTimer{}";
    }
}

