package protocols.agreement.raft.utils;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class RaftSerialization {

    private RaftSerialization() {
    }

    public static void writeHost(ByteBuf out, Host host) {
        byte[] address = host.getAddress().getAddress();
        out.writeInt(address.length);
        out.writeBytes(address);
        out.writeInt(host.getPort());
    }

    public static Host readHost(ByteBuf in) throws UnknownHostException {
        byte[] address = new byte[in.readInt()];
        in.readBytes(address);
        int port = in.readInt();
        return new Host(InetAddress.getByAddress(address), port);
    }

    public static void writeLogEntry(ByteBuf out, LogEntry entry) {
        out.writeInt(entry.getTerm());
        out.writeInt(entry.getIndex());
        out.writeLong(entry.getOpId().getMostSignificantBits());
        out.writeLong(entry.getOpId().getLeastSignificantBits());
        out.writeInt(entry.getOperation().length);
        out.writeBytes(entry.getOperation());
    }

    public static LogEntry readLogEntry(ByteBuf in) {
        int term = in.readInt();
        int index = in.readInt();
        UUID opId = new UUID(in.readLong(), in.readLong());
        byte[] operation = new byte[in.readInt()];
        in.readBytes(operation);
        return new LogEntry(term, index, opId, operation);
    }

    public static void writeLogEntries(ByteBuf out, List<LogEntry> entries) {
        out.writeInt(entries.size());
        for (LogEntry entry : entries) {
            writeLogEntry(out, entry);
        }
    }

    public static List<LogEntry> readLogEntries(ByteBuf in) {
        int count = in.readInt();
        List<LogEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(readLogEntry(in));
        }
        return entries;
    }
}
