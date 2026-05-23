package protocols.agreement.multiPaxos.notifications;

import java.util.List;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class JoinedNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 102;

    private final List<Host> membership;
    private final int joinInstance;

    public JoinedNotification(List<Host> membership, int joinInstance) {
        super(NOTIFICATION_ID);
        this.membership = membership;
        this.joinInstance = joinInstance;
    }

    public int getJoinInstance() {
        return joinInstance;
    }

    public List<Host> getMembership() {
        return membership;
    }

    @Override
    public String toString() {
        return "JoinedNotification{" +
                "membership=" + membership +
                ", joinInstance=" + joinInstance +
                '}';
    }
}
