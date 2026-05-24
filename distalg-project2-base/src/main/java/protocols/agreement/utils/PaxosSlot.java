package protocols.agreement.utils;

import java.io.Serializable;
import java.util.UUID;

public class PaxosSlot implements Serializable {
    private static final long serialVersionUID = 1L;

    private Ballot highestPrepareSeen; // Maior ballot/termo visto na Fase 1 para este slot
    private Ballot highestAcceptSeen;  // Maior ballot/termo aceite na Fase 2
    private UUID opId;
    private byte[] acceptedValue;   // O valor (operação) atualmente aceite neste slot
    private boolean isDecided;      // Se este slot já alcançou consenso
    
    public PaxosSlot() {
        this.highestPrepareSeen = null;
        this.highestAcceptSeen = null;
        this.opId = null;
        this.acceptedValue = null;
        this.isDecided = false;
    }

    public Ballot getHighestPrepareSeen() { return highestPrepareSeen; }
    public Ballot getHighestAcceptSeen() { return highestAcceptSeen; }
    public UUID getOpId() { return opId; }
    public byte[] getAcceptedValue() { return acceptedValue; }
    public boolean getIsDecided() { return isDecided; }

    public void setHighestPrepareSeen(Ballot highestPrepareSeen) { this.highestPrepareSeen = highestPrepareSeen; }
    public void setHighestAcceptSeen(Ballot highestAcceptSeen) { this.highestAcceptSeen = highestAcceptSeen; }
    public void setOpId(UUID opId) { this.opId = opId; }
    public void setAcceptedValue(byte[] acceptedValue) { this.acceptedValue = acceptedValue; }
    public void setDecided(boolean isDecided) { this.isDecided = isDecided; }
}