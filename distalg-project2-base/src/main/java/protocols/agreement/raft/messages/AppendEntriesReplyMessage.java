package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

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
        public void serialize(AppendEntriesReplyMessage msg, ByteBuf out) {
            out.writeInt(msg.term);
            out.writeBoolean(msg.success);
            out.writeInt(msg.matchIndex);
        }

        @Override
        public AppendEntriesReplyMessage deserialize(ByteBuf in) {
            return new AppendEntriesReplyMessage(in.readInt(), in.readBoolean(), in.readInt());
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
