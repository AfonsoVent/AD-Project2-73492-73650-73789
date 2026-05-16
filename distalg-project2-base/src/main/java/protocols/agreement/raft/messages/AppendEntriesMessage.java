package protocols.agreement.raft.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import java.io.IOException;
import java.util.List;

/**
 * AppendEntries RPC message
 * Used by leader to replicate log entries and send heartbeats
 */
public class AppendEntriesMessage extends ProtoMessage {

    public static final short MSG_ID = 1001;

    private final int term;
    private final int leaderId;
    private final int prevLogIndex;
    private final int prevLogTerm;
    private final List<byte[]> entries;
    private final int leaderCommit;

    public AppendEntriesMessage(int term, int leaderId, int prevLogIndex, int prevLogTerm, 
                               List<byte[]> entries, int leaderCommit) {
        super(MSG_ID);
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public int getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<byte[]> getEntries() {
        return entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public static final ISerializer<AppendEntriesMessage> serializer = new ISerializer<AppendEntriesMessage>() {
        @Override
        public void serialize(AppendEntriesMessage msg, pt.unl.fct.di.novasys.network.data.IMutableBuffer out) throws IOException {
            // TODO: Implement serialization
        }

        @Override
        public AppendEntriesMessage deserialize(pt.unl.fct.di.novasys.network.data.ImmutableBuffer in) throws IOException {
            // TODO: Implement deserialization
            return null;
        }
    };

    @Override
    public String toString() {
        return "AppendEntriesMessage{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entriesCount=" + entries.size() +
                ", leaderCommit=" + leaderCommit +
                '}';
    }
}

