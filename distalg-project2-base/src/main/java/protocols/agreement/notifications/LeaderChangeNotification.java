package protocols.agreement.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class LeaderChangeNotification extends ProtoNotification {
	
	public final static short NOTIFICATION_ID = 103;
	
	private final Host leaderID;
	
	public LeaderChangeNotification(Host leader) {
		super(NOTIFICATION_ID);
		this.leaderID = leader;
	}

	public Host getLeaderID() {
		return this.leaderID;
	}
	
}
