package org.example.benchmark;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * High-performance throughput and latency microbenchmark for a simulated 'aether-packet-engine'.
 * Measures ops/sec and exact p50/p99 latency percentiles across high iterations.
 * Outputs ONLY raw Java code.
 */
public final class AetherPacketEngineBenchmark {

    // Configuration constants
    private static final int WARMUP_ITERATIONS = 5_000_000;
    private static final int MEASUREMENT_ITERATIONS = 20_000_000;
    private static final int BATCH_SIZE = 1024;

    // Simulated Packet Engine Core
    public static final class AetherPacketEngine {
        private long processedBytes = 0;

        public final void processPacket(final byte[] packet, final int length) {
            // Simulate packet parsing, validation, and routing lookup logic
            long checksum = 0;
            for (int i = 0; i < length; i++) {
                checksum += packet[i];
            }
            // Prevent JVM dead-code elimination
            this.processedBytes += checksum;
        }
    }

    public static void main(String[] args) {
        final AetherPacketEngine engine = new AetherPacketEngine();
        final byte[] samplePacket = new byte[256];
        ThreadLocalRandom.current().nextBytes(samplePacket);

        System.out.println("Initializing Aether Packet Engine Microbenchmark...");
        System.out.printf("Warmup Iterations: %,d%n", WARMUP_ITERATIONS);
        System.out.printf("Measurement Iterations: %,d%n", MEASUREMENT_ITERATIONS);
        System.out.println("--------------------------------------------------");

        // 1. Warmup phase to trigger JIT compilation and tiered optimization
        long warmupDummy = 0;
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            engine.processPacket(samplePacket, samplePacket.length);
            warmupDummy += engine.processedBytes;
        }
        if (warmupDummy == -1) {
            System.out.println("Unreachable");
        }

        // Force garbage collection prior to measurement
        System.gc();

        // 2. Measurement phase with fine-grained latency recording
        // To avoid storing 20 million individual timestamps (which causes massive memory overhead/GC pauses),
        // we sample latency on every Nth iteration, or record raw nanoseconds into a pre-allocated primitive array.
        // For p50/p99 precision at scale, we record every iteration's delta using a ring buffer or direct array 
        // if memory permits. 20M longs ~ 160MB, completely safe for modern heaps.
        
        final long[] latencies = new long[MEASUREMENT_ITERATIONS];
        
        // Pre-load to eliminate JIT/paging anomalies during run
        Arrays.fill(latencies, 0L);

        final long startTime = System.nanoTime();

        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            final long startNano = System.nanoTime();
            
            engine.processPacket(samplePacket, samplePacket.length);
            
            final long endNano = System.nanoTime();
            latencies[i] = endNano - startNano;
        }

        final long endTime = System.nanoTime();
        final long totalDurationNs = endTime - startTime;

        // 3. Post-processing metrics
        Arrays.sort(latencies);

        final double totalDurationSec = (double) totalDurationNs / TimeUnit.SECONDS.toNanos(1);
        final double opsPerSec = MEASUREMENT_ITERATIONS / totalDurationSec;
        
        final double meanLatencyNs = (double) totalDurationNs / MEASUREMENT_ITERATIONS;
        final long p50Ns = latencies[(int) (MEASUREMENT_ITERATIONS * 0.50)];
        final long p90Ns = latencies[(int) (MEASUREMENT_ITERATIONS * 0.90)];
        final long p99Ns = latencies[(int) (MEASUREMENT_ITERATIONS * 0.99)];
        final long p999Ns = latencies[Math.min((int) (MEASUREMENT_ITERATIONS * 0.999), MEASUREMENT_ITERATIONS - 1)];

        // 4. Output results in a clear formatted tabular layout to stdout
        System.out.println("\n==================================================");
        System.out.println("      AETHER-PACKET-ENGINE BENCHMARK RESULTS      ");
        System.out.println("==================================================");
        System.out.printf("%-25s | %-20s%n", "Metric", "Result");
        System.out.println("--------------------------------------------------");
        System.out.printf("%-25s | %,15.2f ops/sec%n", "Throughput", opsPerSec);
        System.out.printf("%-25s | %,15.2f ms%n", "Total Duration", totalDurationSec * 1000.0);
        System.out.printf("%-25s | %,15d%n", "Total Packets Processed", MEASUREMENT_ITERATIONS);
        System.out.println("--------------------------------------------------");
        System.out.printf("%-25s | %,15.2f ns%n", "Mean Latency", meanLatencyNs);
        System.out.printf("%-25s | %,15.2f µs%n", "p50 Latency (Median)", p50Ns / 1000.0);
        System.out.printf("%-25s | %,15.2f µs%n", "p90 Latency", p90Ns / 1000.0);
        System.out.printf("%-25s | %,15.2f µs%n", "p99 Latency", p99Ns / 1000.0);
        System.out.printf("%-25s | %,15.2f µs%n", "p99.9 Latency", p999Ns / 1000.0);
        System.out.println("==================================================");
        
        // Prevent engine reference clearing
        if (engine.processedBytes == 0) {
            System.err.println("Warning: Engine output was zero.");
        }
    }
}