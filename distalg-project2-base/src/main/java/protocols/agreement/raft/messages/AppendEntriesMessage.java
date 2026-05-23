package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import protocols.agreement.raft.utils.LogEntry;
import protocols.agreement.raft.utils.RaftSerialization;

import java.util.List;

public class AppendEntriesMessage extends ProtoMessage {

    public static final short MSG_ID = 1001;
    private static final Logger logger = LogManager.getLogger(AppendEntriesMessage.class);

    private final int term;
    private final Host leaderId;
    private final int prevLogIndex;
    private final int prevLogTerm;
    private final List<LogEntry> entries;
    private final int leaderCommit;

    public AppendEntriesMessage(int term, Host leaderId, int prevLogIndex, int prevLogTerm,
                                List<LogEntry> entries, int leaderCommit) {
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

    public Host getLeaderId() {
        return leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public static final ISerializer<AppendEntriesMessage> serializer = new ISerializer<AppendEntriesMessage>() {
        @Override
        public void serialize(AppendEntriesMessage msg, ByteBuf out) {
            out.writeInt(msg.term);
            RaftSerialization.writeHost(out, msg.leaderId);
            out.writeInt(msg.prevLogIndex);
            out.writeInt(msg.prevLogTerm);
            RaftSerialization.writeLogEntries(out, msg.entries);
            out.writeInt(msg.leaderCommit);
        }

        @Override
        public AppendEntriesMessage deserialize(ByteBuf in) {
            try {
                int term = in.readInt();
                Host leader = RaftSerialization.readHost(in);
                int prevLogIndex = in.readInt();
                int prevLogTerm = in.readInt();
                List<LogEntry> entries = RaftSerialization.readLogEntries(in);
                int leaderCommit = in.readInt();
                return new AppendEntriesMessage(term, leader, prevLogIndex, prevLogTerm, entries, leaderCommit);
            } catch (Exception e) {
                logger.error("Failed to deserialize AppendEntriesMessage", e);
                return null;
            }
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
