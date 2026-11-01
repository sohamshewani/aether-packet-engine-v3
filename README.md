# aether-packet-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/aether-packet-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/aether-packet-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/aether-packet-engine) [![Stars](https://img.shields.io/github/stars/sohamshewani/aether-packet-engine?style=flat)](https://github.com/sohamshewani/aether-packet-engine)

## Executive Summary & Problem Space

Modern high-throughput networking pipelines demand deterministic, ultra-low latency packet inspection and transformation at line rate. Traditional Java-based packet processors frequently succumb to garbage collection (GC) pressure, unsafe memory layout overhead, and lock contention across multi-threaded ingestion rings. 

`aether-packet-engine` is an enterprise-grade, high-performance packet processing framework engineered in Java. By combining zero-copy buffer slicing, off-heap memory management patterns, and lock-free ring buffers, `aether-packet-engine` eliminates runtime allocations on the hot path. It solves the fundamental bottlenecks of throughput degradation and latency tail-spikes (p99.99), making it suitable for carrier-grade software-defined networking (SDN), edge security gateways, and high-frequency telemetry parsers.

---

## Structural ASCII Architecture / Dataflow Diagram

The system operates on a staged processing pipeline designed to isolate packet ingestion, validation, transformation, and dispatch across decoupled thread domains.

```
+-----------------------------------------------------------------------------------+
|                           INGESTION & HARDWARE INTERFACE                          |
|  [Network Interface / DPDK / Socket Channel]                                      |
+-----------------------------------------+-----------------------------------------+
                                          | Raw Byte Stream
                                          v
+-----------------------------------------------------------------------------------+
|                        RING BUFFER & ZERO-COPY BOUNDARY                           |
|  +-----------------------------------------------------------------------------+  |
|  |                Lock-Free SPSC Ring Buffer (Off-Heap Arena)                  |  |
|  +--------------------------------------+--------------------------------------+  |
+-----------------------------------------+-----------------------------------------+
                                          | Direct ByteBuffer Slice
                                          v
+-----------------------------------------------------------------------------------+
|                        PARSING & METADATA EXTRACTION                              |
|  +-----------------------------------------------------------------------------+  |
|  |  core/types/PacketHeader.java                                               |  |
|  |  - Zero-allocation header decoding (Ethernet / IPv4 / TCP / UDP)            |  |
|  |  - Bitmask isolation & endianness normalization                             |  |
|  +--------------------------------------+--------------------------------------+  |
+-----------------------------------------+-----------------------------------------+
                                          | Immutable PacketContext
                                          v
+-----------------------------------------------------------------------------------+
|                           PARALLEL WORKER POOL (LMAX)                             |
|  [Worker Thread 1] ----> [Stateful Firewall Filter] ---> [Telemetry Emitter]      |
|  [Worker Thread 2] ----> [Deep Packet Inspection]   ---> [Route Table Lookups]    |
+-----------------------------------------+-----------------------------------------+
                                          | Processed Event Stream
                                          v
+-----------------------------------------------------------------------------------+
|                             DISPATCH & EGRESS SUBSYSTEM                           |
|  [Output Queue / Sink Interface / Drop Engine]                                    |
+-----------------------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

`aether-packet-engine` enforces strict concurrency and memory invariants to prevent data races and garbage collection pauses during high-load operations.

*   **Lock-Free Single-Producer Single-Consumer (SPSC) Rings:** Ingestion paths leverage memory-mapped or off-heap ring buffers utilizing atomic sequence numbers (`VarHandle` primitives with acquire-release semantics) to eliminate mutex acquisition overhead.
*   **Memory Ownership & Immutability:** Once raw bytes are parsed into `PacketHeader` structures (`core/types/PacketHeader.java`), the underlying payload references are treated as immutable. Workers operate on localized copies or read-only slices, preventing cross-thread state corruption.
*   **Off-Heap Arena Allocation:** Direct byte buffers and memory segments bypass the JVM heap where possible, shielding the engine from Stop-The-World (STW) garbage collection events during sustained peak traffic bursts.
*   **False Sharing Mitigation:** Sequence barriers and ring buffer state indexes are padded with cache-line-sized boundaries (`@Contended` annotations or manual padding) to prevent CPU cache line bouncing across physical cores.

---

## Performance Characteristics & Algorithmic Complexity Table

The core algorithms within `aether-packet-engine` are engineered for bounded, predictable execution times (deterministic latency profiles).

| Operation Path | Algorithmic Complexity (Time) | Algorithmic Complexity (Space) | Estimated p99 Latency | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Ingestion & Framing** | $O(1)$ | $O(1)$ | $< 120\text{ns}$ | Ring buffer push/pop via atomic head/tail advancement. |
| **Header Parsing** | $O(1)$ | $O(1)$ | $< 250\text{ns}$ | Bitwise masking and structural offset mapping without object creation. |
| **Flow Table Lookup** | $O(1)$ (Expected) / $O(N)$ (Worst) | $O(N)$ | $< 400\text{ns}$ | Lock-free hash map with robin-hood probing or Hopscotch hashing. |
| **Packet Routing / Merge** | $O(\log K)$ | $O(1)$ | $< 600\text{ns}$ | Trie-based longest prefix matching (LPM) for IP forwarding. |

---

## Security Architecture & Threat Model

Packet engines operate at the absolute perimeter of systems infrastructure, making them primary targets for malformed packet injection, denial of service (DoS), and memory exhaustion vectors.

*   **Input Boundary Hardening:** Every incoming byte array or direct buffer is subjected to strict length and boundary checks before slice creation. Negative offsets, truncated headers, and malformed option fields trigger immediate, safe rejection paths without throwing uncaught runtime exceptions.
*   **Memory Corruption Protections:** By restricting direct native pointer manipulation and utilizing Java's strict array bounds checking combined with off-heap limits, buffer-overflow and use-after-free vulnerabilities native to C/C++ implementations are structurally eliminated.
*   **Resource Exhaustion Mitigation (DoS):** Ingestion rings employ bounded capacities with drop-tail or Explicit Congestion Notification (ECN) backpressure policies. This prevents unbounded memory allocation under high-volume SYN-flood or packet-storm scenarios.
*   **Sanitization and Validation:** Invariant verification scripts (`tests/test_invariants.py`) run continuously in CI pipelines to validate protocol compliance, ensuring edge-case header combinations cannot induce infinite loops or integer overflows within the parsing logic.

---

## Build, Verification & Benchmarking Recipes

To compile the codebase, execute rigorous invariant tests, and run micro-benchmarks, execute the following shell commands from the project root.

### 1. Compilation & Test Suite Execution
Compile the Maven project and run the complete test suite, including invariant validation scripts:

```bash
# Clean, compile, and execute standard unit and integration tests via Maven
mvn clean verify

# Execute Python-based system and invariant verification tests
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip pytest
pytest tests/test_invariants.py -v
```

### 2. Running Performance Benchmarks
Execute the performance benchmarking suite to measure throughput, parsing latency, and pipeline overhead:

```bash
# Ensure benchmark dependencies and Python runtime are active
pip install numpy matplotlib
python3 benchmarks/bench_engine.py --iterations 1000000 --concurrency 4