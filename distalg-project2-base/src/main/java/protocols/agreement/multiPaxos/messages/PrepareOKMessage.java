package protocols.agreement.multiPaxos.messages;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import protocols.agreement.multiPaxos.utils.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareOKMessage extends ProtoMessage {
    public static final short MSG_ID = 104;

    private final Ballot ballot;
    private final Map<Integer, SlotStateData> acceptedSlots;

    public PrepareOKMessage(Ballot ballot, Map<Integer, SlotStateData> acceptedSlots) {
        super(MSG_ID);
        this.ballot = ballot;
        this.acceptedSlots = acceptedSlots;
    }

    public Ballot getBallot() { return ballot; }
    public Map<Integer, SlotStateData> getAcceptedSlots() { return acceptedSlots; }

    public static class SlotStateData {
        public Ballot highestAcceptSeen;
        public byte[] value;

        public SlotStateData(Ballot highestAcceptSeen, byte[] value) {
            this.highestAcceptSeen = highestAcceptSeen;
            this.value = value;
        }
    }

    public static ISerializer<PrepareOKMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PrepareOKMessage msg, ByteBuf out) {
            try {
                Ballot.serializer.serialize(msg.ballot, out);
                out.writeInt(msg.acceptedSlots.size());
                for (Map.Entry<Integer, SlotStateData> entry : msg.acceptedSlots.entrySet()) {
                    out.writeInt(entry.getKey());
                    Ballot.serializer.serialize(entry.getValue().highestAcceptSeen, out);
                    if (entry.getValue().value == null) {
                        out.writeInt(-1);
                    } else {
                        out.writeInt(entry.getValue().value.length);
                        out.writeBytes(entry.getValue().value);
                    }
            }} catch (IOException e) {
                throw new RuntimeException("Error serializing PrepareOKMessage", e);
            }
        }

        @Override
        public PrepareOKMessage deserialize(ByteBuf in) {
            try { 
                Ballot ballot = Ballot.serializer.deserialize(in);
                int size = in.readInt();
                Map<Integer, SlotStateData> slots = new HashMap<>();
                for (int i = 0; i < size; i++) {
                    int slotId = in.readInt();
                    Ballot acceptSeen = Ballot.serializer.deserialize(in);
                    int len = in.readInt();
                    byte[] val = null;
                    if (len != -1) {
                        val = new byte[len];
                        in.readBytes(val);
                    }
                    slots.put(slotId, new SlotStateData(acceptSeen, val));
                }
                return new PrepareOKMessage(ballot, slots);
        } catch (IOException e) {
                throw new RuntimeException("Error deserializing PrepareOKMessage", e);    
        }}
    };
}