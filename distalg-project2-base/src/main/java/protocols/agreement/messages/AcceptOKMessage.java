package protocols.agreement.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import protocols.agreement.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AcceptOKMessage extends ProtoMessage {
    public static final short MSG_ID = 106;

    private final Ballot ballot;
    private final int instance;

    public AcceptOKMessage(Ballot ballot, int instance) {
        super(MSG_ID);
        this.ballot = ballot;
        this.instance = instance;
    }

    public Ballot getBallot() { return ballot; }
    public int getInstance() { return instance; }

    public static ISerializer<AcceptOKMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(AcceptOKMessage msg, ByteBuf out) {
            try {
                Ballot.serializer.serialize(msg.ballot, out);
                out.writeInt(msg.instance);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing AcceptOKMessage", e);
            }
        }

        @Override
        public AcceptOKMessage deserialize(ByteBuf in) {
            try {
                Ballot ballot = Ballot.serializer.deserialize(in);
                int instance = in.readInt();
                return new AcceptOKMessage(ballot, instance);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing AcceptOKMessage", e);
            }
        }
    };
}