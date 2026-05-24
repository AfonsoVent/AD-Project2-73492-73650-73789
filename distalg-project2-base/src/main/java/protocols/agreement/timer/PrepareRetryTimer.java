package protocols.agreement.timer;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class PrepareRetryTimer extends ProtoTimer {
    public static final short TIMER_ID = 403;

    private final int generation;

    public PrepareRetryTimer(int generation) {
        super(TIMER_ID);
        this.generation = generation;
    }

    public int getGeneration() { return generation; }

    @Override
    public ProtoTimer clone() { return this; }
}