package protocols.agreement.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AcceptMessage extends ProtoMessage {
    public static final short MSG_ID = 105;

    private final int term;
    private final int instance;
    private final UUID opId;
    private final byte[] value;

    public AcceptMessage(int term, int instance, UUID opId, byte[] value) {
        super(MSG_ID);
        this.term = term;
        this.instance = instance;
        this.opId = opId;
        this.value = value;
    }

    public int getTerm() {return term;}
    public int getInstance() {return instance;}
    public UUID getOpId() {return opId;}
    public byte[] getValue() {return value;}

    public static ISerializer<AcceptMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(AcceptMessage msg, ByteBuf out) {
            out.writeInt(msg.term);
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.value.length);
            out.writeBytes(msg.value);
        }

        @Override
        public AcceptMessage deserialize(ByteBuf in) {
            int term = in.readInt();
            int instance = in.readInt();
            long mostSig = in.readLong();
            long leastSig = in.readLong();
            UUID opId = new UUID(mostSig, leastSig);
            byte[] value = new byte[in.readInt()];
            in.readBytes(value);
            return new AcceptMessage(term, instance, opId, value);
        }
    };
}