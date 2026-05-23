package protocols.agreement.multiPaxos.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.utils.AgreementSerializationUtils;
import protocols.agreement.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class DecideMessage extends ProtoMessage {

    public static final short MSG_ID = 115;

    private final Ballot ballot;
    private final int instance;
    private final UUID opId;
    private final byte[] operation;

    public DecideMessage(Ballot ballot, int instance, UUID opId, byte[] operation) {
        super(MSG_ID);
        this.ballot = ballot;
        this.instance = instance;
        this.opId = opId;
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

    public static final ISerializer<DecideMessage> serializer = new ISerializer<DecideMessage>() {
        @Override
        public void serialize(DecideMessage msg, ByteBuf out) {
            AgreementSerializationUtils.writeBallot(msg.ballot, out);
            out.writeInt(msg.instance);
            AgreementSerializationUtils.writeUUID(msg.opId, out);
            AgreementSerializationUtils.writeBytes(msg.operation, out);
        }

        @Override
        public DecideMessage deserialize(ByteBuf in) {
            Ballot ballot = AgreementSerializationUtils.readBallot(in);
            int instance = in.readInt();
            UUID opId = AgreementSerializationUtils.readUUID(in);
            byte[] operation = AgreementSerializationUtils.readBytes(in);
            return new DecideMessage(ballot, instance, opId, operation);
        }
    };
}