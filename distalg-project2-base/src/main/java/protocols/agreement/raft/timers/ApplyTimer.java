package protocols.agreement.raft.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Apply Timer
 * Used to periodically apply committed entries to the state machine
 */
public class ApplyTimer extends ProtoTimer {

    public static final short TIMER_ID = 2003;

    public ApplyTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

    @Override
    public String toString() {
        return "ApplyTimer{}";
    }
}

