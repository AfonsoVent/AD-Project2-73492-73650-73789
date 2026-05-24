package protocols.agreement.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

public final class AgreementSerializationUtils {

    private AgreementSerializationUtils() {
    }

    public static void writeHost(Host host, ByteBuf out) {
        byte[] address = host.getAddress().getHostAddress().getBytes(StandardCharsets.UTF_8);
        out.writeInt(address.length);
        out.writeBytes(address);
        out.writeInt(host.getPort());
    }

    public static Host readHost(ByteBuf in) {
        byte[] address = new byte[in.readInt()];
        in.readBytes(address);
        int port = in.readInt();
        try {
            return new Host(InetAddress.getByName(new String(address, StandardCharsets.UTF_8)), port);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Failed to deserialize host", e);
        }
    }

    public static void writeUUID(UUID uuid, ByteBuf out) {
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
    }

    public static UUID readUUID(ByteBuf in) {
        return new UUID(in.readLong(), in.readLong());
    }

    public static void writeBytes(byte[] bytes, ByteBuf out) {
        out.writeInt(bytes.length);
        out.writeBytes(bytes);
    }

    public static byte[] readBytes(ByteBuf in) {
        byte[] bytes = new byte[in.readInt()];
        in.readBytes(bytes);
        return bytes;
    }

    public static void writeBallot(Ballot ballot, ByteBuf out) {
        out.writeLong(ballot.getCounter());
        writeUUID(ballot.getProposer(), out);
    }

    public static Ballot readBallot(ByteBuf in) {
        long counter = in.readLong();
        UUID proposer = readUUID(in);
        return new Ballot(counter, proposer);
    }

    public static int compareHosts(Host left, Host right) {
        if (left == right) return 0;
        if (left == null) return -1;
        if (right == null) return 1;
        int addressComparison = left.getAddress().getHostAddress().compareTo(right.getAddress().getHostAddress());
        if (addressComparison != 0) return addressComparison;
        return Integer.compare(left.getPort(), right.getPort());
    }
}