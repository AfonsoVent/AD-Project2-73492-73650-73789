package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class ForwardOpMessage extends ProtoMessage {

    public static final short MSG_ID = 301;

    private final UUID opId;
    private final byte[] operation;

    public ForwardOpMessage(
        UUID opId, 
        byte[] operation) {
            super(MSG_ID);
            
            this.opId = opId;
            this.operation = operation;
    }

    public UUID getOpId() {return opId;}
    public byte[] getOperation() {return operation;}

    @Override
    public String toString() {
        return "ForwardOpMessage{" +
                "opId=" + opId +
                ", operation=" + Hex.encodeHexString(operation) +
                '}';
    }

    public static ISerializer<ForwardOpMessage> serializer = new ISerializer<ForwardOpMessage>() {
        @Override
        public void serialize(ForwardOpMessage msg, ByteBuf out) {
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.operation.length);
            out.writeBytes(msg.operation);
        }

        @Override
        public ForwardOpMessage deserialize(ByteBuf in) {
            long high = in.readLong();
            long low = in.readLong();
            UUID opId = new UUID(high, low);

            byte[] op = new byte[in.readInt()];
            in.readBytes(op);

            return new ForwardOpMessage(opId, op);
        }
    };
}