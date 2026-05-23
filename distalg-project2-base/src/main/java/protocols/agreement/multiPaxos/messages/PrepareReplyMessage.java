package protocols.agreement.multiPaxos.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.utils.AgreementSerializationUtils;
import protocols.agreement.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class PrepareReplyMessage extends ProtoMessage {

    public static final short MSG_ID = 112;

    public static final class AcceptedValue {
        private final int instance;
        private final Ballot ballot;
        private final UUID opId;
        private final byte[] operation;

        public AcceptedValue(int instance, Ballot ballot, UUID opId, byte[] operation) {
            this.instance = instance;
            this.ballot = ballot;
            this.opId = opId;
            this.operation = operation;
        }

        public int getInstance() {
            return instance;
        }

        public Ballot getBallot() {
            return ballot;
        }

        public UUID getOpId() {
            return opId;
        }

        public byte[] getOperation() {
            return operation;
        }
    }

    private final Ballot ballot;
    private final boolean ok;
    private final List<AcceptedValue> acceptedValues;

    public PrepareReplyMessage(Ballot ballot, boolean ok, List<AcceptedValue> acceptedValues) {
        super(MSG_ID);
        this.ballot = ballot;
        this.ok = ok;
        this.acceptedValues = acceptedValues == null ? Collections.emptyList() : acceptedValues;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public boolean isOk() {
        return ok;
    }

    public List<AcceptedValue> getAcceptedValues() {
        return acceptedValues;
    }

    public static final ISerializer<PrepareReplyMessage> serializer = new ISerializer<PrepareReplyMessage>() {
        @Override
        public void serialize(PrepareReplyMessage msg, ByteBuf out) {
            AgreementSerializationUtils.writeBallot(msg.ballot, out);
            out.writeBoolean(msg.ok);
            out.writeInt(msg.acceptedValues.size());
            for (AcceptedValue value : msg.acceptedValues) {
                out.writeInt(value.instance);
                AgreementSerializationUtils.writeBallot(value.ballot, out);
                AgreementSerializationUtils.writeUUID(value.opId, out);
                AgreementSerializationUtils.writeBytes(value.operation, out);
            }
        }

        @Override
        public PrepareReplyMessage deserialize(ByteBuf in) {
            Ballot ballot = AgreementSerializationUtils.readBallot(in);
            boolean ok = in.readBoolean();
            int size = in.readInt();
            List<AcceptedValue> acceptedValues = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int instance = in.readInt();
                Ballot acceptedBallot = AgreementSerializationUtils.readBallot(in);
                UUID opId = AgreementSerializationUtils.readUUID(in);
                byte[] operation = AgreementSerializationUtils.readBytes(in);
                acceptedValues.add(new AcceptedValue(instance, acceptedBallot, opId, operation));
            }
            return new PrepareReplyMessage(ballot, ok, acceptedValues);
        }
    };
}