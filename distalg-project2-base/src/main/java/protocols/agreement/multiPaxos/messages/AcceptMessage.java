<<<<<<< HEAD:distalg-project2-base/src/main/java/protocols/agreement/multiPaxos/messages/AcceptMessage.java
package protocols.agreement.multiPaxos.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.agreement.multiPaxos.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AcceptMessage extends ProtoMessage {
    public static final short MSG_ID = 105;
=======
package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.utils.AgreementSerializationUtils;
import protocols.agreement.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class AcceptMessage extends ProtoMessage {

    public static final short MSG_ID = 113;
>>>>>>> origin:distalg-project2-base/src/main/java/protocols/agreement/messages/AcceptMessage.java

    private final Ballot ballot;
    private final int instance;
    private final UUID opId;
<<<<<<< HEAD:distalg-project2-base/src/main/java/protocols/agreement/multiPaxos/messages/AcceptMessage.java
    private final byte[] value;

    public AcceptMessage(Ballot ballot, int instance, UUID opId, byte[] value) {
=======
    private final byte[] operation;

    public AcceptMessage(Ballot ballot, int instance, UUID opId, byte[] operation) {
>>>>>>> origin:distalg-project2-base/src/main/java/protocols/agreement/messages/AcceptMessage.java
        super(MSG_ID);
        this.ballot = ballot;
        this.instance = instance;
        this.opId = opId;
<<<<<<< HEAD:distalg-project2-base/src/main/java/protocols/agreement/multiPaxos/messages/AcceptMessage.java
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
=======
        this.operation = operation;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public int getInstance() {
        return instance;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOperation() {
        return operation;
    }

    public static final ISerializer<AcceptMessage> serializer = new ISerializer<AcceptMessage>() {
        @Override
        public void serialize(AcceptMessage msg, ByteBuf out) {
            AgreementSerializationUtils.writeBallot(msg.ballot, out);
            out.writeInt(msg.instance);
            AgreementSerializationUtils.writeUUID(msg.opId, out);
            AgreementSerializationUtils.writeBytes(msg.operation, out);
>>>>>>> origin:distalg-project2-base/src/main/java/protocols/agreement/messages/AcceptMessage.java
        }

        @Override
        public AcceptMessage deserialize(ByteBuf in) {
<<<<<<< HEAD:distalg-project2-base/src/main/java/protocols/agreement/multiPaxos/messages/AcceptMessage.java
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
=======
            Ballot ballot = AgreementSerializationUtils.readBallot(in);
            int instance = in.readInt();
            UUID opId = AgreementSerializationUtils.readUUID(in);
            byte[] operation = AgreementSerializationUtils.readBytes(in);
            return new AcceptMessage(ballot, instance, opId, operation);
>>>>>>> origin:distalg-project2-base/src/main/java/protocols/agreement/messages/AcceptMessage.java
        }
    };
}