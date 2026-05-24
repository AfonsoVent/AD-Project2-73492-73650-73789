package protocols.agreement.utils;

public final class QuorumUtils {
    private QuorumUtils() {}

    public static int majority(int numberOfReplicas) {
        return (numberOfReplicas / 2) + 1;
    }

    public static boolean hasMajority(int votes, int numberOfReplicas) {
        return votes >= majority(numberOfReplicas);
    }
}