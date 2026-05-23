package protocols.agreement.multiPaxos.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.utils.AgreementSerializationUtils;
import protocols.agreement.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class LeaderAnnouncementMessage extends ProtoMessage {

    public static final short MSG_ID = 116;

    private final Ballot ballot;

    public LeaderAnnouncementMessage(Ballot ballot) {
        super(MSG_ID);
        this.ballot = ballot;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public static final ISerializer<LeaderAnnouncementMessage> serializer = new ISerializer<LeaderAnnouncementMessage>() {
        @Override
        public void serialize(LeaderAnnouncementMessage msg, ByteBuf out) {
            AgreementSerializationUtils.writeBallot(msg.ballot, out);
        }

        @Override
        public LeaderAnnouncementMessage deserialize(ByteBuf in) {
            return new LeaderAnnouncementMessage(AgreementSerializationUtils.readBallot(in));
        }
    };
}