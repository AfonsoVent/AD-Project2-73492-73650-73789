package protocols.agreement.utils;

import java.io.Serializable;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * Ballot identifier used for Paxos ballots.
 * A ballot is a pair (counter, proposerId) and supports comparison.
 */
public final class Ballot implements Comparable<Ballot>, Serializable {
    private static final long serialVersionUID = 1L;

    private final long counter;
    private final UUID proposer;

    public Ballot(long counter, UUID proposer) {
        this.counter = counter;
        this.proposer = proposer;
    }

    public long getCounter() { return counter; }
    public UUID getProposer() { return proposer; }
    public Ballot next() { return new Ballot(counter + 1, proposer); }

    public static Ballot of(long counter, UUID proposer) { return new Ballot(counter, proposer); }
    public static Ballot initial(UUID proposer) { return new Ballot(0L, proposer); }

    @Override
    public int compareTo(Ballot o) {
        int cmp = Long.compare(this.counter, o.counter);
        if (cmp != 0) return cmp;

        int msbCmp = Long.compare(this.proposer.getMostSignificantBits(), o.proposer.getMostSignificantBits());
        if (msbCmp != 0) return msbCmp;
        return Long.compare(this.proposer.getLeastSignificantBits(), o.proposer.getLeastSignificantBits());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Ballot)) return false;
        Ballot o = (Ballot) obj;
        return this.counter == o.counter && this.proposer.equals(o.proposer);
    }

    @Override
    public int hashCode() {
        int h = Long.hashCode(counter);
        h = 31 * h + proposer.hashCode();
        return h;
    }

    @Override
    public String toString() {
        return "Ballot{" + "counter=" + counter + ", proposer=" + proposer + '}';
    }

    public static final ISerializer<Ballot> serializer = new ISerializer<>() {
        @Override
        public void serialize(Ballot ballot, ByteBuf out) {
            out.writeLong(ballot.counter);
            out.writeLong(ballot.proposer.getMostSignificantBits());
            out.writeLong(ballot.proposer.getLeastSignificantBits());
        }

        @Override
        public Ballot deserialize(ByteBuf in) {
            long counter = in.readLong();
            long msb = in.readLong();
            long lsb = in.readLong();
            return new Ballot(counter, new UUID(msb, lsb));
        }
    };
}
