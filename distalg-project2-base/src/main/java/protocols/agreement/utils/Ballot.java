package protocols.agreement.utils;

import java.util.Objects;

import pt.unl.fct.di.novasys.network.data.Host;

public final class Ballot implements Comparable<Ballot> {

    private final long round;
    private final Host proposer;

    public Ballot(long round, Host proposer) {
        this.round = round;
        this.proposer = proposer;
    }

    public long getRound() {
        return round;
    }

    public Host getProposer() {
        return proposer;
    }

    @Override
    public int compareTo(Ballot other) {
        if (other == null) return 1;
        int comparison = Long.compare(round, other.round);
        if (comparison != 0) return comparison;
        return AgreementSerializationUtils.compareHosts(proposer, other.proposer);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof Ballot)) return false;
        Ballot ballot = (Ballot) object;
        return round == ballot.round && Objects.equals(proposer, ballot.proposer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(round, proposer);
    }

    @Override
    public String toString() {
        return "Ballot{" +
                "round=" + round +
                ", proposer=" + proposer +
                '}';
    }
}