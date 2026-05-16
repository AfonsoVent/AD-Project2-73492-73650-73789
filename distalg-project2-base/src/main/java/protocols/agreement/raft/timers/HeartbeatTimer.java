package protocols.agreement.raft.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Heartbeat Timer
 * Used by leader to send periodic heartbeats to followers
 */
public class HeartbeatTimer extends ProtoTimer {

    public static final short TIMER_ID = 2002;

    public HeartbeatTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

    @Override
    public String toString() {
        return "HeartbeatTimer{}";
    }
}

