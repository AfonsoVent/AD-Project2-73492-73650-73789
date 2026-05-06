package protocols.statemachine.utils;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class PendingOp {
    private final UUID opId;
    private final byte[] operation;
    private final Host origin;

    public PendingOp(
        UUID opId, 
        byte[] operation, 
        Host origin) {
            this.opId = opId;
            this.operation = operation;
            this.origin = origin;
    }

    public UUID getOpId() {return opId;}

    public byte[] getOperation() {return operation;}

    public Host getOrigin() {return origin;}

    @Override
    public String toString() {
        return "PendingOp{" +
                "opId=" + opId +
                ", origin=" + origin +
                ", operationSize=" + (operation == null ? 0 : operation.length) +
                '}';
    }
}