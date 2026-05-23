package protocols.agreement.utils;

import java.util.UUID;

public class PaxosSlot {
    private int highestPrepareSeen; // Maior ballot/termo visto na Fase 1 para este slot
    private int highestAcceptSeen;  // Maior ballot/termo aceite na Fase 2
    private UUID opId;
    private byte[] acceptedValue;   // O valor (operação) atualmente aceite neste slot
    private boolean isDecided;      // Se este slot já alcançou consenso
    
    public PaxosSlot() {
        this.highestPrepareSeen = -1;
        this.highestAcceptSeen = -1;
        this.opId = null;
        this.acceptedValue = null;
        this.isDecided = false;
    }

    public int getHighestPrepareSeen() {return highestPrepareSeen;}
    public int getHighestAcceptSeen() {return highestAcceptSeen;}
    public UUID getOpId() {return opId;}
    public byte[] getAcceptedValue() {return acceptedValue;}
    public boolean getIsDecided() {return isDecided;}

    public void setHighestPrepareSeen(int highestPrepareSeen) {this.highestPrepareSeen = highestPrepareSeen;}
    public void setHighestAcceptSeen(int highestAcceptSeen) {this.highestAcceptSeen = highestAcceptSeen;}
    public void setOpId(UUID opId) {this.opId = opId;}
    public void setAcceptedValue(byte[] acceptedValue) {this.acceptedValue = acceptedValue;}
    public void setDecided(boolean isDecided) {this.isDecided = isDecided;}
}