package protocols.agreement.raft.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import java.io.IOException;

/**
 * RequestVote RPC message
 * Used by candidates to request votes for leader election
 */
public class RequestVoteMessage extends ProtoMessage {

    public static final short MSG_ID = 1003;

    private final int term;
    private final int candidateId;
    private final int lastLogIndex;
    private final int lastLogTerm;

    public RequestVoteMessage(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
        super(MSG_ID);
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() {
        return term;
    }

    public int getCandidateId() {
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
        public void serialize(RequestVoteMessage msg, pt.unl.fct.di.novasys.network.data.IMutableBuffer out) throws IOException {
            // TODO: Implement serialization
        }

        @Override
        public RequestVoteMessage deserialize(pt.unl.fct.di.novasys.network.data.ImmutableBuffer in) throws IOException {
            // TODO: Implement deserialization
            return null;
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

