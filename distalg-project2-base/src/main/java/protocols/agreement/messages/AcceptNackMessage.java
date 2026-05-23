package protocols.agreement.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import protocols.agreement.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AcceptNackMessage extends ProtoMessage {
    public static final short MSG_ID = 107;
    public final Ballot promised;
    public final int instance;

    public AcceptNackMessage(Ballot promised, int instance) {
        super(MSG_ID);
        this.promised = promised;
        this.instance = instance;
    }

    public Ballot getPromised() { return promised; }
    public int getInstance() { return instance; }

    public static final ISerializer<AcceptNackMessage> serializer = new ISerializer<AcceptNackMessage>() {
        @Override
        public void serialize(AcceptNackMessage m, ByteBuf out) {
            try {
                Ballot.serializer.serialize(m.promised, out);
                out.writeInt(m.instance);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing AcceptNackMessage", e);
            }
        }

        @Override
        public AcceptNackMessage deserialize(ByteBuf in) {
            try {
                Ballot b = Ballot.serializer.deserialize(in);
                int i = in.readInt();
                return new AcceptNackMessage(b, i);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing AcceptNackMessage", e);
            }
        }       
    };
}