package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.utils.AgreementSerializationUtils;
import protocols.agreement.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage extends ProtoMessage {

    public static final short MSG_ID = 111;

    private final Ballot ballot;

    public PrepareMessage(Ballot ballot) {
        super(MSG_ID);
        this.ballot = ballot;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public static final ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            AgreementSerializationUtils.writeBallot(msg.ballot, out);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            return new PrepareMessage(AgreementSerializationUtils.readBallot(in));
        }
    };
}