package com.aether.packet.engine;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive unit, invariant, and concurrent stress-test suite for 'aether-packet-engine'.
 * Designed to validate zero-copy semantics, memory bounds, thread safety, high-contention
 * ring-buffer performance, and failure recovery under adverse concurrency.
 */
public class AetherPacketEngineTest {

    // ==========================================
    // STUBS & ENGINE IMPLEMENTATIONS
    // ==========================================

    public static final int DEFAULT_MTU = 1500;
    public static final int MIN_HEADER_SIZE = 14;

    public enum PacketType {
        DATA(1), ACK(2), CONTROL(3), HEARTBEAT(4), CORRUPT(255);
        private final int id;
        PacketType(int id) { this.id = id; }
        public static PacketType fromId(int id) {
            for (PacketType t : values()) if (t.id == id) return t;
            throw new IllegalArgumentException("Unknown packet type ID: " + id);
        }
    }

    public static class AetherPacket {
        private final ByteBuffer buffer;
        private final int capacity;
        private boolean isReleased;

        public AetherPacket(int capacity) {
            this.capacity = capacity;
            this.buffer = ByteBuffer.allocateDirect(capacity);
            this.isReleased = false;
        }

        public AetherPacket(ByteBuffer externalBuffer) {
            this.capacity = externalBuffer.capacity();
            this.buffer = externalBuffer;
            this.isReleased = false;
        }

        public synchronized ByteBuffer getBuffer() {
            ensureNotReleased();
            return buffer;
        }

        public synchronized int getCapacity() {
            return capacity;
        }

        public synchronized boolean isReleased() {
            return isReleased;
        }

        public synchronized void release() {
            if (!isReleased) {
                isReleased = true;
                buffer.clear();
            }
        }

        private void ensureNotReleased() {
            if (isReleased) {
                throw new IllegalStateException("Attempted to access released AetherPacket");
            }
        }
    }

    public interface PacketInterceptor {
        boolean intercept(AetherPacket packet);
    }

    public static class PacketEngineConfig {
        private int mtu = DEFAULT_MTU;
        private int poolSize = 1024;
        private boolean checksumEnabled = true;

        public PacketEngineConfig setMtu(int mtu) { this.mtu = mtu; return this; }
        public int getMtu() { return mtu; }
        public PacketEngineConfig setPoolSize(int poolSize) { this.poolSize = poolSize; return this; }
        public int getPoolSize() { return poolSize; }
        public PacketEngineConfig setChecksumEnabled(boolean checksumEnabled) { this.checksumEnabled = checksumEnabled; return this; }
        public boolean isChecksumEnabled() { return checksumEnabled; }
    }

    public static class PacketEngineException extends RuntimeException {
        public PacketEngineException(String message) { super(message); }
        public PacketEngineException(String message, Throwable cause) { super(message, cause); }
    }

    public static class AetherPacketEngine {
        private final PacketEngineConfig config;
        private final Queue<AetherPacket> packetPool;
        private final List<PacketInterceptor> interceptors;
        private final AtomicLong packetsProcessed = new AtomicLong(0);
        private final AtomicLong packetsDropped = new AtomicLong(0);
        private final AtomicLong checksumErrors = new AtomicLong(0);
        private final AtomicBoolean isRunning = new AtomicBoolean(true);

        public AetherPacketEngine(PacketEngineConfig config) {
            this.config = config;
            this.packetPool = new ConcurrentLinkedQueue<>();
            this.interceptors = new CopyOnWriteArrayList<>();
            
            for (int i = 0; i < config.getPoolSize(); i++) {
                packetPool.add(new AetherPacket(config.getMtu()));
            }
        }

        public AetherPacket acquirePacket() {
            ensureRunning();
            AetherPacket packet = packetPool.poll();
            if (packet == null) {
                packetsDropped.incrementAndGet();
                throw new PacketEngineException("Packet pool exhausted. Capacity: " + config.getPoolSize());
            }
            if (packet.isReleased()) {
                // Re-instantiate if a released packet slipped back in
                packet = new AetherPacket(config.getMtu());
            }
            return packet;
        }

        public void recyclePacket(AetherPacket packet) {
            if (packet == null) return;
            packet.release();
            if (packetPool.size() < config.getPoolSize() * 2) {
                packetPool.offer(new AetherPacket(config.getMtu()));
            }
        }

        public boolean processPacket(AetherPacket packet) {
            ensureRunning();
            if (packet == null || packet.isReleased()) {
                packetsDropped.incrementAndGet();
                return false;
            }

            ByteBuffer buf = packet.getBuffer();
            buf.flip();

            if (buf.remaining() < MIN_HEADER_SIZE) {
                packetsDropped.incrementAndGet();
                return false;
            }

            if (config.isChecksumEnabled()) {
                if (!validateChecksum(buf)) {
                    checksumErrors.incrementAndGet();
                    packetsDropped.incrementAndGet();
                    return false;
                }
            }

            for (PacketInterceptor interceptor : interceptors) {
                try {
                    if (!interceptor.intercept(packet)) {
                        packetsDropped.incrementAndGet();
                        return false;
                    }
                } catch (Exception e) {
                    packetsDropped.incrementAndGet();
                    throw new PacketEngineException("Interceptor failure", e);
                }
            }

            packetsProcessed.incrementAndGet();
            return true;
        }

        public void addInterceptor(PacketInterceptor interceptor) {
            interceptors.add(interceptor);
        }

        public void removeInterceptor(PacketInterceptor interceptor) {
            interceptors.remove(interceptor);
        }

        public void shutdown() {
            if (isRunning.compareAndSet(true, false)) {
                packetPool.clear();
                interceptors.clear();
            }
        }

        public boolean isRunning() {
            return isRunning.get();
        }

        public long getPacketsProcessed() { return packetsProcessed.get(); }
        public long getPacketsDropped() { return packetsDropped.get(); }
        public long getChecksumErrors() { return checksumErrors.get(); }

        private boolean validateChecksum(ByteBuffer buf) {
            int originalPosition = buf.position();
            try {
                int limit = buf.limit();
                if (limit < 4) return false;
                
                int expectedChecksum = buf.getInt(limit - 4);
                int calculated = 0;
                for (int i = 0; i < limit - 4; i++) {
                    calculated += buf.get(i) & 0xFF;
                }
                return calculated == expectedChecksum;
            } finally {
                buf.position(originalPosition);
            }
        }

        public static int computeChecksum(ByteBuffer buf, int length) {
            int sum = 0;
            int originalPos = buf.position();
            for (int i = 0; i < length; i++) {
                sum += buf.get(originalPos + i) & 0xFF;
            }
            return sum;
        }

        private void ensureRunning() {
            if (!isRunning.get()) {
                throw new PacketEngineException("AetherPacketEngine is shut down.");
            }
        }
    }

    // ==========================================
    // TEST SUITE
    // ==========================================

    private AetherPacketEngine engine;
    private PacketEngineConfig config;

    @BeforeEach
    void setUp() {
        config = new PacketEngineConfig().setMtu(DEFAULT_MTU).setPoolSize(128);
        engine = new AetherPacketEngine(config);
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    // -----------------------------------------
    // 1. BOUNDARY & EDGE-CASE ASSERTIONS
    // -----------------------------------------

    @Test
    @DisplayName("Boundary: Acquire and release baseline validations")
    void testAcquireAndReleaseBoundaries() {
        AetherPacket packet = engine.acquirePacket();
        assertNotNull(packet);
        assertFalse(packet.isReleased());
        assertEquals(DEFAULT_MTU, packet.getCapacity());

        engine.recyclePacket(packet);
        assertTrue(packet.isReleased());

        // Accessing released packet must throw IllegalStateException
        assertThrows(IllegalStateException.class, packet::getBuffer);
    }

    @Test
    @DisplayName("Edge Case: Zero-length and undersized packet processing")
    void testUndersizedPacketDrop() {
        AetherPacket packet = engine.acquirePacket();
        ByteBuffer buf = packet.getBuffer();
        buf.put((byte) 0x01); // Only 1 byte written, less than MIN_HEADER_SIZE (14)
        
        boolean result = engine.processPacket(packet);
        assertFalse(result);
        assertEquals(1, engine.getPacketsDropped());
        assertEquals(0, engine.getPacketsProcessed());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 64, DEFAULT_MTU, DEFAULT_MTU * 2})
    @DisplayName("Boundary: Custom MTU boundaries initialization")
    void testCustomMtuBoundaries(int mtu) {
        if (mtu <= 0) {
            assertThrows(IllegalArgumentException.class, () -> new PacketEngineConfig().setMtu(mtu));
            return;
        }
        PacketEngineConfig customConfig = new PacketEngineConfig().setMtu(mtu).setPoolSize(10);
        AetherPacketEngine customEngine = new AetherPacketEngine(customConfig);
        
        AetherPacket packet = customEngine.acquirePacket();
        assertEquals(mtu, packet.getCapacity());
        customEngine.shutdown();
    }

    @Test
    @DisplayName("Edge Case: Checksum validation success and corruption detection")
    void testChecksumBoundaries() {
        AetherPacket packet = engine.acquirePacket();
        ByteBuffer buf = packet.getBuffer();

        // Write a valid packet with header and checksum
        for (int i = 0; i < MIN_HEADER_SIZE; i++) {
            buf.put((byte) 0xAA);
        }
        int checksum = AetherPacketEngine.computeChecksum(buf, MIN_HEADER_SIZE);
        buf.putInt(checksum);

        boolean result = engine.processPacket(packet);
        assertTrue(result, "Valid checksum packet should pass");
        assertEquals(1, engine.getPacketsProcessed());

        // Test corrupted checksum
        AetherPacket badPacket = engine.acquirePacket();
        ByteBuffer badBuf = badPacket.getBuffer();
        for (int i = 0; i < MIN_HEADER_SIZE; i++) {
            badBuf.put((byte) 0xAA);
        }
        badBuf.putInt(checksum + 999); // Invalid checksum

        boolean badResult = engine.processPacket(badPacket);
        assertFalse(badResult, "Corrupted checksum packet should fail");
        assertEquals(1, engine.getChecksumErrors());
    }

    // -----------------------------------------
    // 2. MULTI-THREADED CONTENTION TESTING
    // -----------------------------------------

    @Test
    @DisplayName("Concurrency: High-contention concurrent packet acquisition and recycling")
    void testConcurrentPoolContention() throws InterruptedException {
        int threadCount = 32;
        int iterationsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger exceptionCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < iterationsPerThread;