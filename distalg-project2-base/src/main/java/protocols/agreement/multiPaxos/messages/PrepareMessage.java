<<<<<<< HEAD:distalg-project2-base/src/main/java/protocols/agreement/multiPaxos/messages/PrepareMessage.java
package protocols.agreement.multiPaxos.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import protocols.agreement.multiPaxos.utils.Ballot;
=======
package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.utils.AgreementSerializationUtils;
import protocols.agreement.utils.Ballot;
>>>>>>> origin:distalg-project2-base/src/main/java/protocols/agreement/messages/PrepareMessage.java
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage extends ProtoMessage {
<<<<<<< HEAD:distalg-project2-base/src/main/java/protocols/agreement/multiPaxos/messages/PrepareMessage.java
    public static final short MSG_ID = 103;
=======

    public static final short MSG_ID = 111;
>>>>>>> origin:distalg-project2-base/src/main/java/protocols/agreement/messages/PrepareMessage.java

    private final Ballot ballot;

    public PrepareMessage(Ballot ballot) {
        super(MSG_ID);
        this.ballot = ballot;
    }

<<<<<<< HEAD:distalg-project2-base/src/main/java/protocols/agreement/multiPaxos/messages/PrepareMessage.java
    public Ballot getBallot() { return ballot; }
    
    public static ISerializer<PrepareMessage> serializer = new ISerializer<>(){
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            try {
                Ballot.serializer.serialize(msg.ballot, out);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing PrepareMessage", e);
            }
=======
    public Ballot getBallot() {
        return ballot;
    }

    public static final ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            AgreementSerializationUtils.writeBallot(msg.ballot, out);
>>>>>>> origin:distalg-project2-base/src/main/java/protocols/agreement/messages/PrepareMessage.java
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
<<<<<<< HEAD:distalg-project2-base/src/main/java/protocols/agreement/multiPaxos/messages/PrepareMessage.java
            try {
                return new PrepareMessage(Ballot.serializer.deserialize(in));
            } catch (IOException e) {
                    throw new RuntimeException("Error serializing PrepareMessage", e);
            }
=======
            return new PrepareMessage(AgreementSerializationUtils.readBallot(in));
>>>>>>> origin:distalg-project2-base/src/main/java/protocols/agreement/messages/PrepareMessage.java
        }
    };
}