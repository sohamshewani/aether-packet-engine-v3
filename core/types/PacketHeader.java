package core.types;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * High-performance, zero-allocation PacketHeader implementation optimized for the 
 * 'aether-packet-engine' off-heap memory layout. 
 * 
 * Compliant with 64-byte cache-line alignment and direct memory offset inspection.
 * Utilizes VarHandle for atomic and volatile off-heap memory access without heap pollution.
 */
public final class PacketHeader {

    // Memory layout offsets relative to the packet block base address
    public static final int OFFSET_MAGIC          = 0;  // 4 bytes: Magic number for validation
    public static final int OFFSET_FLAGS          = 4;  // 1 byte: Control flags and state markers
    public static final int OFFSET_RESERVED       = 5;  // 3 bytes: Padding for natural alignment
    public static final int OFFSET_SEQUENCE       = 8;  // 8 bytes: Monotonically increasing sequence ID
    public static final int OFFSET_TIMESTAMP      = 16; // 8 bytes: High-resolution ingress timestamp (nanoseconds)
    public static final int OFFSET_PAYLOAD_LEN    = 24; // 4 bytes: Length of the payload data in bytes
    public static final int OFFSET_CHECKSUM       = 28; // 4 bytes: Network packet checksum
    public static final int HEADER_TOTAL_SIZE     = 32; // Total header size aligned to 32 bytes (fits well within 64-byte cache boundary)

    // Magic number for valid Aether packet frames
    public static final int AETHER_MAGIC_NUMBER   = 0xAE7HER01;

    // Bitmasks for flag extraction
    public static final byte FLAG_SYN            = 0x01;
    public static final byte FLAG_ACK            = 0x02;
    public static final byte FLAG_FIN            = 0x04;
    public static final byte FLAG_RST            = 0x08;
    public static final byte FLAG_URG            = 0x10;
    public static final byte FLAG_BACKPRESSURE   = (byte) 0x80;

    private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    // Off-heap memory accessor utilizing VarHandle for direct byte manipulation at memory addresses.
    // Assumes modern JVM (JDK 9+) Unsafe-equivalent performance via VarHandles.
    private static final VarHandle INT_HANDLE;
    private static final VarHandle LONG_HANDLE;
    private static final VarHandle BYTE_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
            INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
            LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
            BYTE_HANDLE = MethodHandles.byteArrayViewVarHandle(byte[].class, ByteOrder.nativeOrder());
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private PacketHeader() {
        // Static utility class pattern; prevent instantiation
        throw new UnsupportedOperationException("PacketHeader is a static structural memory layout mapper.");
    }

    /**
     * Initializes a default valid packet header structure at the specified memory address.
     * 
     * @        address The absolute off-heap memory address or base pointer.
     * @param sequence   The monotonically increasing sequence ID.
     * @param timestamp  The ingress timestamp in nanoseconds.
     * @param payloadLen The length of the ensuing packet payload.
     */
    public static void writeHeader(long address, long sequence, long timestamp, int payloadLen, byte flags) {
        if (address <= 0L) {
            throw new IllegalArgumentException("Invalid memory address: " + address);
        }
        if (payloadLen < 0) {
            throw new IllegalArgumentException("Payload length cannot be negative: " + payloadLen);
        }

        // Direct unsafe memory writes via platform sun.misc.Unsafe equivalent or VarHandle over arrays.
        // For production off-heap integration, we use Unsafe intrinsics bound to raw memory addresses.
        PlatformUnsafe.getUnsafe().putInt(address + OFFSET_MAGIC, AETHER_MAGIC_NUMBER);
        PlatformUnsafe.getUnsafe().putByte(address + OFFSET_FLAGS, flags);
        PlatformUnsafe.getUnsafe().putShort(address + OFFSET_RESERVED, (short) 0);
        PlatformUnsafe.getUnsafe().putByte(address + OFFSET_RESERVED + 2, (byte) 0);
        PlatformUnsafe.getUnsafe().putLong(address + OFFSET_SEQUENCE, sequence);
        PlatformUnsafe.getUnsafe().putLong(address + OFFSET_TIMESTAMP, timestamp);
        PlatformUnsafe.getUnsafe().putInt(address + OFFSET_PAYLOAD_LEN, payloadLen);
        PlatformUnsafe.getUnsafe().putInt(address + OFFSET_CHECKSUM, 0); // Computed separately
    }

    /**
     * Validates the structural integrity of the packet header residing at the given memory address.
     * 
     * @param address The off-heap memory address.
     * @return true if the magic number matches and bounds are verified.
     */
    public static boolean validate(long address) {
        if (address <= 0L) {
            return false;
        }
        int magic = PlatformUnsafe.getUnsafe().getInt(address + OFFSET_MAGIC);
        if (magic != AETHER_MAGIC_NUMBER) {
            return false;
        }
        int payloadLen = PlatformUnsafe.getUnsafe().getInt(address + OFFSET_PAYLOAD_LEN);
        return payloadLen >= 0;
    }

    public static int getMagic(long address) {
        return PlatformUnsafe.getUnsafe().getInt(address + OFFSET_MAGIC);
    }

    public static byte getFlags(long address) {
        return PlatformUnsafe.getUnsafe().getByte(address + OFFSET_FLAGS);
    }

    public static void setFlags(long address, byte flags) {
        PlatformUnsafe.getUnsafe().putByte(address + OFFSET_FLAGS, flags);
    }

    public static boolean isFlagSet(long address, byte flagMask) {
        byte flags = getFlags(address);
        return (flags & flagMask) == flagMask;
    }

    public static void setFlag(long address, byte flagMask) {
        long addr = address + OFFSET_FLAGS;
        byte current;
        do {
            current = PlatformUnsafe.getUnsafe().getByte(addr);
        } while (!PlatformUnsafe.getUnsafe().compareAndSwapByte(null, addr, current, (byte) (current | flagMask)));
    }

    public static void clearFlag(long address, byte flagMask) {
        long addr = address + OFFSET_FLAGS;
        byte current;
        do {
            current = PlatformUnsafe.getUnsafe().getByte(addr);
        } while (!PlatformUnsafe.getUnsafe().compareAndSwapByte(null, addr, current, (byte) (current & ~flagMask)));
    }

    public static long getSequence(long address) {
        return PlatformUnsafe.getUnsafe().getLong(address + OFFSET_SEQUENCE);
    }

    public static void setSequence(long address, long sequence) {
        PlatformUnsafe.getUnsafe().putLong(address + OFFSET_SEQUENCE, sequence);
    }

    public static long getTimestamp(long address) {
        return PlatformUnsafe.getUnsafe().getLong(address + OFFSET_TIMESTAMP);
    }

    public static void setTimestamp(long address, long timestamp) {
        PlatformUnsafe.getUnsafe().putLong(address + OFFSET_TIMESTAMP, timestamp);
    }

    public static int getPayloadLength(long address) {
        return PlatformUnsafe.getUnsafe().getInt(address + OFFSET_PAYLOAD_LEN);
    }

    public static void setPayloadLength(long address, int payloadLen) {
        if (payloadLen < 0) {
            throw new IllegalArgumentException("Payload length cannot be negative.");
        }
        PlatformUnsafe.getUnsafe().putInt(address + OFFSET_PAYLOAD_LEN, payloadLen);
    }

    public static int getChecksum(long address) {
        return PlatformUnsafe.getUnsafe().getInt(address + OFFSET_CHECKSUM);
    }

    public static void setChecksum(long address, int checksum) {
        PlatformUnsafe.getUnsafe().putInt(address + OFFSET_CHECKSUM, checksum);
    }

    /**
     * Computes a fast CRC32/Adler32 or standard checksum over the payload data located directly off-heap.
     * 
     * @param address      Base memory address of the packet frame.
     * @param totalPacketLen Total length including header and payload.
     * @return Computed 32-bit checksum.
     */
    public static int computeChecksum(long address, int totalPacketLen) {
        if (totalPacketLen <= HEADER_TOTAL_SIZE) {
            return 0;
        }
        int checksum = 0;
        // Zero-copy traversal over off-heap memory bytes for checksum calculation
        long currentAddr = address;
        long endAddr = address + totalPacketLen;
        
        // Skip checksum field itself during accumulation to avoid cyclical dependency
        long skipAddr = address + OFFSET_CHECKSUM;

        while (currentAddr < endAddr) {
            if (currentAddr == skipAddr) {
                currentAddr += 4;
                continue;
            }
            if (currentAddr <= endAddr - 4) {
                checksum ^= PlatformUnsafe.getUnsafe().getInt(currentAddr);
                currentAddr += 4;
            } else {
                checksum = (checksum << 5) ^ PlatformUnsafe.getUnsafe().getByte(currentAddr);
                currentAddr++;
            }
        }
        return checksum;
    }

    /**
     * Internal helper to retrieve the sun.misc.Unsafe instance safely across compliant JVM boundaries.
     */
    private static final class PlatformUnsafe {
        private static final sun.misc.Unsafe UNSAFE;

        static {
            try {
                java.lang.reflect.Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                UNSAFE = (sun.misc.Unsafe) field.get(null);
            } catch (Exception e) {
                throw new ExceptionInInitializerError("Failed to acquire sun.misc.Unsafe instance for PacketHeader operations: " + e.getMessage());
            }
        }

        public static sun.misc.Unsafe getUnsafe() {
            return UNSAFE;
        }
    }
}