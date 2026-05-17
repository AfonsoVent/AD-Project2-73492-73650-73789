package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

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
        public void serialize(RequestVoteReplyMessage msg, ByteBuf out) {
            out.writeInt(msg.term);
            out.writeBoolean(msg.voteGranted);
        }

        @Override
        public RequestVoteReplyMessage deserialize(ByteBuf in) {
            return new RequestVoteReplyMessage(in.readInt(), in.readBoolean());
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
