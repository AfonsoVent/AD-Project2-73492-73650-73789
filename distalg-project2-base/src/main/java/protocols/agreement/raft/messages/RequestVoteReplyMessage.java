package protocols.agreement.raft.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import java.io.IOException;

/**
 * RequestVote Reply message
 * Response to RequestVote RPC
 */
public class RequestVoteReplyMessage extends ProtoMessage {

    public static final short MSG_ID = 1004;

    private final int term;
    private final boolean voteGranted;

    public RequestVoteReplyMessage(int term, boolean voteGranted) {
        super(MSG_ID);
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public static final ISerializer<RequestVoteReplyMessage> serializer = new ISerializer<RequestVoteReplyMessage>() {
        @Override
        public void serialize(RequestVoteReplyMessage msg, pt.unl.fct.di.novasys.network.data.IMutableBuffer out) throws IOException {
            // TODO: Implement serialization
        }

        @Override
        public RequestVoteReplyMessage deserialize(pt.unl.fct.di.novasys.network.data.ImmutableBuffer in) throws IOException {
            // TODO: Implement deserialization
            return null;
        }
    };

    @Override
    public String toString() {
        return "RequestVoteReplyMessage{" +
                "term=" + term +
                ", voteGranted=" + voteGranted +
                '}';
    }
}

