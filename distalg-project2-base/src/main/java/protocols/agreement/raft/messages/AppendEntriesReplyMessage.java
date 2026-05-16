package protocols.agreement.raft.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import java.io.IOException;

/**
 * AppendEntries Reply message
 * Response to AppendEntries RPC
 */
public class AppendEntriesReplyMessage extends ProtoMessage {

    public static final short MSG_ID = 1002;

    private final int term;
    private final boolean success;
    private final int matchIndex;

    public AppendEntriesReplyMessage(int term, boolean success, int matchIndex) {
        super(MSG_ID);
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public static final ISerializer<AppendEntriesReplyMessage> serializer = new ISerializer<AppendEntriesReplyMessage>() {
        @Override
        public void serialize(AppendEntriesReplyMessage msg, pt.unl.fct.di.novasys.network.data.IMutableBuffer out) throws IOException {
            // TODO: Implement serialization
        }

        @Override
        public AppendEntriesReplyMessage deserialize(pt.unl.fct.di.novasys.network.data.ImmutableBuffer in) throws IOException {
            // TODO: Implement deserialization
            return null;
        }
    };

    @Override
    public String toString() {
        return "AppendEntriesReplyMessage{" +
                "term=" + term +
                ", success=" + success +
                ", matchIndex=" + matchIndex +
                '}';
    }
}

