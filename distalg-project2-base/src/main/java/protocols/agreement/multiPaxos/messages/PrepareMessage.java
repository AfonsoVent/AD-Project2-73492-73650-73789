package protocols.agreement.multiPaxos.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import protocols.agreement.multiPaxos.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage extends ProtoMessage {
    public static final short MSG_ID = 103;

    private final Ballot ballot;

    public PrepareMessage(Ballot ballot) {
        super(MSG_ID);
        this.ballot = ballot;
    }

    public Ballot getBallot() { return ballot; }
    
    public static ISerializer<PrepareMessage> serializer = new ISerializer<>(){
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            try {
                Ballot.serializer.serialize(msg.ballot, out);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing PrepareMessage", e);
            }
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            try {
                return new PrepareMessage(Ballot.serializer.deserialize(in));
            } catch (IOException e) {
                    throw new RuntimeException("Error serializing PrepareMessage", e);
            }
        }
    };
}