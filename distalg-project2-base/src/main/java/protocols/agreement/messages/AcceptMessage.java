package protocols.agreement.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.agreement.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AcceptMessage extends ProtoMessage {
    public static final short MSG_ID = 105;

    private final Ballot ballot;
    private final int instance;
    private final UUID opId;
    private final byte[] value;

    public AcceptMessage(Ballot ballot, int instance, UUID opId, byte[] value) {
        super(MSG_ID);
        this.ballot = ballot;
        this.instance = instance;
        this.opId = opId;
        this.value = value;
    }

    public Ballot getBallot() { return ballot; }
    public int getInstance() { return instance; }
    public UUID getOpId() { return opId; }
    public byte[] getValue() { return value; }

    public static ISerializer<AcceptMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(AcceptMessage msg, ByteBuf out) {
            try {
                Ballot.serializer.serialize(msg.ballot, out);
                out.writeInt(msg.instance);
                out.writeLong(msg.opId.getMostSignificantBits());
                out.writeLong(msg.opId.getLeastSignificantBits());
                out.writeInt(msg.value.length);
                out.writeBytes(msg.value);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing AcceptMessage", e);
            }
        }

        @Override
        public AcceptMessage deserialize(ByteBuf in) {
            try {
                Ballot ballot = Ballot.serializer.deserialize(in);
                int instance = in.readInt();
                long mostSig = in.readLong();
                long leastSig = in.readLong();
                UUID opId = new UUID(mostSig, leastSig);
                byte[] value = new byte[in.readInt()];
                in.readBytes(value);
                return new AcceptMessage(ballot, instance, opId, value);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing AcceptMessage", e);
            }
        }
    };
}