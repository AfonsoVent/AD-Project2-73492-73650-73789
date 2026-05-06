package protocols.statemachine.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ReconnectTimer extends ProtoTimer {

    public static final short TIMER_ID = 401;

    private final Host target;

    public ReconnectTimer(Host target) {
        super(TIMER_ID);
        this.target = target;
    }

    public Host getTarget() {
        return target;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}