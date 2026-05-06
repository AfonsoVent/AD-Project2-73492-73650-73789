package protocols.statemachine.notifications;

import java.util.UUID;

import org.apache.commons.codec.binary.Hex;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class ClientRequestReply extends ProtoNotification {

    public static final short NOTIFICATION_ID = 202;

    private final UUID cID;
    private final byte[] operation;

    public ClientRequestReply(UUID cID, byte[] operation) {
        super(NOTIFICATION_ID);
        this.cID = cID;
        this.operation = operation;
    }

    public byte[] getOperation() {
        return operation;
    }

    public UUID getOpId() {
        return cID;
    }

    @Override
    public String toString() {
        return "ExecuteNotification{" +
                "opId=" + cID +
                ", operation=" + Hex.encodeHexString(operation) +
                '}';
    }
}
