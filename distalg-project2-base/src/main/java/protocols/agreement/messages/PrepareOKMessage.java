package protocols.agreement.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import java.util.HashMap;
import java.util.Map;

public class PrepareOKMessage extends ProtoMessage {
    public static final short MSG_ID = 104;

    private final int term;
    private final Map<Integer, SlotStateData> acceptedSlots;

    public PrepareOKMessage(int term, Map<Integer, SlotStateData> acceptedSlots) {
        super(MSG_ID);
        this.term = term;
        this.acceptedSlots = acceptedSlots;
    }

    public int getTerm() { return term; }
    public Map<Integer, SlotStateData> getAcceptedSlots() { return acceptedSlots; }

    public static class SlotStateData {
        public int highestAcceptSeen;
        public byte[] value;

        public SlotStateData(int highestAcceptSeen, byte[] value) {
            this.highestAcceptSeen = highestAcceptSeen;
            this.value = value;
        }
    }

    public static ISerializer<PrepareOKMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PrepareOKMessage msg, ByteBuf out) {
            out.writeInt(msg.term);
            out.writeInt(msg.acceptedSlots.size());
            for (Map.Entry<Integer, SlotStateData> entry : msg.acceptedSlots.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeInt(entry.getValue().highestAcceptSeen);
                if (entry.getValue().value == null) {
                    out.writeInt(-1);
                } else {
                    out.writeInt(entry.getValue().value.length);
                    out.writeBytes(entry.getValue().value);
                }
            }
        }

        @Override
        public PrepareOKMessage deserialize(ByteBuf in) {
            int term = in.readInt();
            int size = in.readInt();
            Map<Integer, SlotStateData> slots = new HashMap<>();
            for (int i = 0; i < size; i++) {
                int slotId = in.readInt();
                int acceptSeen = in.readInt();
                int len = in.readInt();
                byte[] val = null;
                if (len != -1) {
                    val = new byte[len];
                    in.readBytes(val);
                }
                slots.put(slotId, new SlotStateData(acceptSeen, val));
            }
            return new PrepareOKMessage(term, slots);
        }
    };
}