package protocols.agreement.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage extends ProtoMessage {
    public static final short MSG_ID = 103;

    private final int term;

    public PrepareMessage(int term) {
        super(MSG_ID);
        this.term = term;
    }

    public int getTerm() { return term; }

    public static ISerializer<PrepareMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            out.writeInt(msg.term);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            return new PrepareMessage(in.readInt());
        }
    };
}