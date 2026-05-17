package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import protocols.agreement.raft.utils.RaftSerialization;

public class RequestVoteMessage extends ProtoMessage {

    public static final short MSG_ID = 1003;
    private static final Logger logger = LogManager.getLogger(RequestVoteMessage.class);

    private final int term;
    private final Host candidateId;
    private final int lastLogIndex;
    private final int lastLogTerm;

    public RequestVoteMessage(int term, Host candidateId, int lastLogIndex, int lastLogTerm) {
        super(MSG_ID);
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() {
        return term;
    }

    public Host getCandidateId() {
        return candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public static final ISerializer<RequestVoteMessage> serializer = new ISerializer<RequestVoteMessage>() {
        @Override
        public void serialize(RequestVoteMessage msg, ByteBuf out) {
            out.writeInt(msg.term);
            RaftSerialization.writeHost(out, msg.candidateId);
            out.writeInt(msg.lastLogIndex);
            out.writeInt(msg.lastLogTerm);
        }

        @Override
        public RequestVoteMessage deserialize(ByteBuf in) {
            try {
                int term = in.readInt();
                Host candidate = RaftSerialization.readHost(in);
                int lastLogIndex = in.readInt();
                int lastLogTerm = in.readInt();
                return new RequestVoteMessage(term, candidate, lastLogIndex, lastLogTerm);
            } catch (Exception e) {
                logger.error("Failed to deserialize RequestVoteMessage", e);
                return null;
            }
        }
    };

    @Override
    public String toString() {
        return "RequestVoteMessage{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
