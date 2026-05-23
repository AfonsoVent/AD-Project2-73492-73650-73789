package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.utils.AgreementSerializationUtils;
import protocols.agreement.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AcceptReplyMessage extends ProtoMessage {

    public static final short MSG_ID = 114;

    private final Ballot ballot;
    private final int instance;
    private final boolean ok;

    public AcceptReplyMessage(Ballot ballot, int instance, boolean ok) {
        super(MSG_ID);
        this.ballot = ballot;
        this.instance = instance;
        this.ok = ok;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public int getInstance() {
        return instance;
    }

    public boolean isOk() {
        return ok;
    }

    public static final ISerializer<AcceptReplyMessage> serializer = new ISerializer<AcceptReplyMessage>() {
        @Override
        public void serialize(AcceptReplyMessage msg, ByteBuf out) {
            AgreementSerializationUtils.writeBallot(msg.ballot, out);
            out.writeInt(msg.instance);
            out.writeBoolean(msg.ok);
        }

        @Override
        public AcceptReplyMessage deserialize(ByteBuf in) {
            Ballot ballot = AgreementSerializationUtils.readBallot(in);
            int instance = in.readInt();
            boolean ok = in.readBoolean();
            return new AcceptReplyMessage(ballot, instance, ok);
        }
    };
}