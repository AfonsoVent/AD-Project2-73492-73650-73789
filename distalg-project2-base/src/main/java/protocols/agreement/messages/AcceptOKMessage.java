package protocols.agreement.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AcceptOKMessage extends ProtoMessage {
    public static final short MSG_ID = 106;

    private final int term;
    private final int instance;

    public AcceptOKMessage(int term, int instance) {
        super(MSG_ID);
        this.term = term;
        this.instance = instance;
    }

    public int getTerm() {return term;}
    public int getInstance() {return instance;}

    public static ISerializer<AcceptOKMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(AcceptOKMessage msg, ByteBuf out) {
            out.writeInt(msg.term);
            out.writeInt(msg.instance);
        }

        @Override
        public AcceptOKMessage deserialize(ByteBuf in) {
            return new AcceptOKMessage(in.readInt(), in.readInt());
        }
    };
}