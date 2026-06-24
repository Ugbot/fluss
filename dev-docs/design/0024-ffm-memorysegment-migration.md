# 0024 — FFM `MemorySegment` Migration (Roadmap 0020 Phase 6)

Status: Design / proposal
Scope: Migrate Fluss's internal `org.apache.fluss.memory.MemorySegment` off `sun.misc.Unsafe`
onto the Java 25 Foreign Function & Memory API (`java.lang.foreign.MemorySegment` /
`java.lang.foreign.Arena` / `java.lang.foreign.ValueLayout`).
Owners: memory subsystem.
Related: `0020-execution-order-and-roadmap.md` (Phase 6, line 139; table line 202), `0023-java25-migration.md`
(`--add-exports`/`--add-opens` removal is gated on this work), `0022-dependency-diet.md`.

> **Naming collision warning.** This document discusses two distinct types both called
> `MemorySegment`:
> - **Fluss `MemorySegment`** = `org.apache.fluss.memory.MemorySegment` (the class we are migrating).
> - **JDK FFM `MemorySegment`** = `java.lang.foreign.MemorySegment` (the new backing primitive).
>
> Throughout this doc the Fluss type is written **`MemorySegment`** (unqualified, in our package)
> and the JDK type is always written **`java.lang.foreign.MemorySegment`** or **FFM segment**.

---

## 0. Why this migration

`sun.misc.Unsafe` is terminally deprecated. JEP 471 (memory-access methods of `Unsafe`) is deprecated
for removal; on JDK 25 those methods log warnings and are slated to be disabled then removed in a
future release. Fluss currently reaches `Unsafe` via reflection (`theUnsafe`), which strong
encapsulation (JEP 403/JEP 396 lineage) increasingly fights. The FFM API
(`java.lang.foreign.*`, finalized in JDK 22 via JEP 454) is the supported replacement and gives us:

- Bounds-checked, JIT-intrinsified `VarHandle`/access primitives with performance parity to `Unsafe`
  in the hot loop (after warmup) when used correctly.
- Deterministic native lifetime via `Arena` instead of reflective `DirectByteBuffer` cleaner tricks
  (`ByteBufferUnmapper`) and GC-driven `Cleaner`.
- Removal of the `--add-exports`/`--add-opens` crutches that `0023-java25-migration.md` keeps "until
  Phase 6". This doc is that Phase 6.

This is a **swap of the backing primitive only**. The Fluss `MemorySegment` public surface must stay
byte-for-byte stable because it sits under the Arrow log codec, binary row format
(`BinarySegmentUtils`, `BinaryArray`), record (de)serialization, and the Kafka typed fetch path.

---

## 1. Inventory of every `Unsafe` operation and its FFM equivalent

### 1.1 Usage sites found (grep of the whole repo, excluding `/target/` and worktree copies)

There are **8 distinct source files** under `fluss-common/src/main` that touch `Unsafe`
(plus 1 test reference and 1 JMH Javadoc reference, and a shaded-Arrow file that is **out of scope** —
see §1.4). Hot-path Fluss-owned files:

| # | File | Lines | Role |
|---|------|-------|------|
| 1 | `fluss-common/src/main/java/org/apache/fluss/memory/MemorySegment.java` | 71, 74, 300, 320, 379, 412, 479, 542, 606, 669, 737, 816, 884, 951, 1281, 1337, 1380, 1413, 1434, 1510, 1514, 1517 | The class being migrated. |
| 2 | `fluss-common/src/main/java/org/apache/fluss/memory/MemoryUtils.java` | 40, 49, 51, 53, 56, 59, 63, 66, 73, 107 | Holds the `UNSAFE` singleton, `objectFieldOffset`, `getByteBufferAddress`. |
| 3 | `fluss-common/src/main/java/org/apache/fluss/row/BinaryArray.java` | 27, 58, 60–65, 618, 619 | Array base offsets; `putInt`/`copyMemory` into `byte[]`. |
| 4 | `fluss-common/src/main/java/org/apache/fluss/row/BinarySegmentUtils.java` | 32, 59 | `BYTE_ARRAY_BASE_OFFSET` + imports `UNSAFE`. |
| 5 | `fluss-common/src/main/java/org/apache/fluss/utils/UnsafeUtils.java` | 29–30, 35, 39, 43, 47, 51, 55, 59, 70, 74, 78, 82 | Heap `byte[]` typed get/put helpers. |
| 6 | `fluss-common/src/main/java/org/apache/fluss/utils/MurmurHashUtils.java` | 103, 127 | `getInt`/`getByte` on an `Object` base + offset. |
| 7 | `fluss-common/src/main/java/org/apache/fluss/utils/log/ByteBufferUnmapper.java` | 148, 152, 154, 155 | Reflective `Unsafe.invokeCleaner` to unmap direct/mapped buffers. |
| 8 | `fluss-common/src/test/java/org/apache/fluss/memory/CrossMemorySegmentTypeTest.java` | 34 | Test reads `arrayBaseOffset`. |

Non-Fluss / informational:
- `fluss-common/.../shaded/arrow/.../ChunkedAllocationManager.java` (lines 168, 202, 271, 324) —
  **shaded Arrow vendored source**, uses `MemoryUtil.UNSAFE.allocateMemory/freeMemory`. Out of scope
  here; handled by the Arrow version bump in `0023` (§1.4).
- `fluss-jmh/.../MemorySegmentBenchmark.java:46` — Javadoc mention only; this is the **perf gate**.

### 1.2 Operation-by-operation mapping (the core of the migration)

The FFM model replaces `(Object base, long absoluteAddress)` Unsafe calls with a
`java.lang.foreign.MemorySegment seg` plus a **relative `long offset`** and a `ValueLayout`. A heap
FFM segment is created with `MemorySegment.ofArray(byte[])` (base is the array, offset is relative,
no `arrayBaseOffset` needed). An off-heap FFM segment is created with `Arena.allocate(...)`.

Endianness is expressed by choosing a `ValueLayout` constant rather than reversing bytes manually
(see §4).

| Current `Unsafe` op (with site) | Semantics | FFM equivalent |
|---|---|---|
| `UNSAFE.arrayBaseOffset(byte[].class)` (MemorySegment:74, BinaryArray:58, BinarySegmentUtils:59, UnsafeUtils:30, CrossMemorySegmentTypeTest:34) | base offset of array data | **Eliminated.** FFM uses relative offsets from a `MemorySegment.ofArray(...)`. No base offset constant needed. |
| `UNSAFE.arrayBaseOffset(boolean/short/int/long/float/double[].class)` (BinaryArray:60–65) | per-type array base offsets for bulk copy into typed arrays | **Eliminated** if we copy via `MemorySegment.ofArray(theArray)` + `MemorySegment.copy`. (See §1.3 — `BinaryArray` typed-array copies map to `MemorySegment.copy(srcSeg, srcOff, dstSeg, dstOff, bytes)`.) |
| `UNSAFE.objectFieldOffset(Field)` (MemoryUtils:73) | offset of `Buffer.address` field | **Eliminated.** Only used to implement `getByteBufferAddress`; FFM replaces address-grabbing with `MemorySegment.ofBuffer(bb)` then `seg.address()` (see §1.3, getByteBufferAddress row). |
| `UNSAFE.getByte(Object base, long pos)` (MemorySegment:300; via UnsafeUtils:70; MurmurHash:127) | read 1 byte | `seg.get(ValueLayout.JAVA_BYTE, offset)` |
| `UNSAFE.putByte(Object, long, byte)` (MemorySegment:320; UnsafeUtils:39) | write 1 byte | `seg.set(ValueLayout.JAVA_BYTE, offset, b)` |
| `UNSAFE.getChar(Object, long)` (MemorySegment:479) | read 2 bytes native-endian | `seg.get(JAVA_CHAR_UNALIGNED, offset)` (native order) |
| `UNSAFE.putChar(Object, long, char)` (MemorySegment:542) | write 2 bytes native-endian | `seg.set(JAVA_CHAR_UNALIGNED, offset, v)` |
| `UNSAFE.getShort(Object, long)` (MemorySegment:606; UnsafeUtils:78) | read 2 bytes native-endian | `seg.get(JAVA_SHORT_UNALIGNED, offset)` |
| `UNSAFE.putShort(Object, long, short)` (MemorySegment:669; UnsafeUtils:43) | write 2 bytes native-endian | `seg.set(JAVA_SHORT_UNALIGNED, offset, v)` |
| `UNSAFE.getInt(Object, long)` (MemorySegment:737; UnsafeUtils:74; MurmurHash:103) | read 4 bytes native-endian | `seg.get(JAVA_INT_UNALIGNED, offset)` |
| `UNSAFE.putInt(Object, long, int)` (MemorySegment:816; UnsafeUtils:47; BinaryArray:618) | write 4 bytes native-endian | `seg.set(JAVA_INT_UNALIGNED, offset, v)` |
| `UNSAFE.getLong(Object, long)` (MemorySegment:884; UnsafeUtils:82) | read 8 bytes native-endian | `seg.get(JAVA_LONG_UNALIGNED, offset)` |
| `UNSAFE.putLong(Object, long, long)` (MemorySegment:951; UnsafeUtils:51) | write 8 bytes native-endian | `seg.set(JAVA_LONG_UNALIGNED, offset, v)` |
| `UNSAFE.putBoolean/putFloat/putDouble` and `getFloat`-style (UnsafeUtils:35/55/59) | typed heap put | `seg.set(JAVA_BOOLEAN, off, v)` / `JAVA_FLOAT_UNALIGNED` / `JAVA_DOUBLE_UNALIGNED` |
| `UNSAFE.getLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET)` (MemoryUtils:107, `getByteBufferAddress`) | grab native address of a direct `ByteBuffer` | `MemorySegment.ofBuffer(directBuffer).address()` — no reflection, no field offset. |
| `UNSAFE.copyMemory(byte[] src, srcPos, byte[]/null dst, dstPos, len)` heap↔heap & heap↔off-heap (MemorySegment:379, 412, 1281, 1337, 1380, 1413, 1434, 1510, 1514, 1517; BinaryArray:619) | bulk byte copy across any base | `MemorySegment.copy(srcSeg, srcOffset, dstSeg, dstOffset, byteCount)` (no element layout = byte-wise, endian-agnostic). For byte[]↔segment also `MemorySegment.copy(byte[], srcIdx, dstSeg, JAVA_BYTE, dstOff, n)` overloads. |
| `Unsafe.allocateMemory(size)` (in shaded Arrow only; Fluss off-heap uses `ByteBuffer.allocateDirect`) | allocate native memory | `arena.allocate(size, alignment)` returns an off-heap FFM segment. |
| `Unsafe.freeMemory(addr)` / `ByteBufferUnmapper.unmap(...)` / `Cleaner` (ByteBufferUnmapper.java) | release native memory deterministically | `arena.close()` frees all segments allocated from that arena atomically. |

Notes on the chosen layouts:

- We use the **`*_UNALIGNED`** `ValueLayout` constants (`JAVA_INT_UNALIGNED`, `JAVA_LONG_UNALIGNED`,
  etc.). The current Unsafe code performs **unaligned** multi-byte access at arbitrary byte offsets
  (e.g. `address + index` with `index` a free `int`); the aligned `ValueLayout.JAVA_INT` would throw
  `IllegalArgumentException` on a misaligned offset. `*_UNALIGNED` is mandatory for byte-addressed
  layouts. See §5 for the perf consequence.
- The current code is **native-endian by default** (`getIntNativeEndian` calls `UNSAFE.getInt`), and
  the public `getInt`/`putInt` flip to LE/BE in Java. We preserve this exactly: native-order layouts
  back the `*NativeEndian` methods, and the existing `LITTLE_ENDIAN`/`reverseBytes` branches stay
  (§4 explains why we keep the manual reverse rather than switching to BE/LE layouts).

### 1.3 `getByteBufferAddress` becomes structurally unnecessary

`getByteBufferAddress` (MemoryUtils:101) exists only so that bulk copies can treat a direct
`ByteBuffer` as a raw pointer for `Unsafe.copyMemory` (MemorySegment:1277, 1281, 1333, 1337). Under
FFM, an off-heap Fluss `MemorySegment` is backed by an FFM segment directly, and a direct
`ByteBuffer` argument is converted with `MemorySegment.ofBuffer(bb)` and copied with
`MemorySegment.copy`. So:

- `MemoryUtils.getByteBufferAddress` and `BUFFER_ADDRESS_FIELD_OFFSET` are **deleted** (no
  reflection on `Buffer.address`).
- `MemorySegment.wrap(int, int)`/`wrapInternal` keep returning a `ByteBuffer` view, but for off-heap
  it returns `ffmSegment.asSlice(offset, length).asByteBuffer()` instead of `duplicate()`-and-set
  position/limit (still a view onto the same memory, semantics preserved).

### 1.4 Explicitly out of scope

- **Shaded Arrow `ChunkedAllocationManager`** (`allocateMemory`/`freeMemory`) is vendored Apache
  Arrow source under `org.apache.fluss.shaded.arrow.*`. Per `0023-java25-migration.md` (lines 247,
  111, 238) this is addressed by **bumping the shaded Arrow version to 17/18**, not by editing
  vendored code. Leave it. It does not import `MemoryUtils.UNSAFE`; it uses Arrow's own
  `MemoryUtil.UNSAFE`.
- `fluss-utils/.../crc/*` and other CRC code: not Unsafe-based; untouched.

---

## 2. Keeping the Fluss `MemorySegment` public API byte-for-byte stable

**Constraint:** the class stays `@Internal public final class MemorySegment`, every existing public
method keeps its exact signature and observable behavior (same return values, same exception types
and messages where tested), and every caller in `BinarySegmentUtils`, `BinaryArray`, the Arrow codec,
and the Kafka path compiles and behaves unchanged.

### 2.1 Field layout change (private, invisible)

Today the segment carries `@Nullable byte[] heapMemory`, `@Nullable ByteBuffer offHeapBuffer`,
mutable `long address`, `long addressLimit`, `int size`. The "freed" sentinel is `address >
addressLimit` (`free()` sets `address = addressLimit + 1`; `isFreed()` checks the inequality;
every accessor re-checks `address > addressLimit`).

New private fields:

```java
private final java.lang.foreign.MemorySegment ffm; // heap (ofArray) or off-heap (arena-allocated) view, size == this.size
@Nullable private final byte[] heapMemory;          // KEPT: non-null iff on-heap (getArray/getHeapMemory/isOffHeap rely on it)
private final int size;                             // KEPT: public size()
private volatile boolean freed;                     // replaces the address>addressLimit sentinel
```

- `heapMemory` is **kept** because `getArray()`, `getHeapMemory()`, and `isOffHeap()` (and bulk
  paths that branch on `heapMemory != null` to use `DataOutput.write(byte[],...)`) are part of the
  observable contract. For heap segments `ffm == MemorySegment.ofArray(heapMemory)`.
- `address`/`addressLimit` are **removed**; bounds checks become `index >= 0 && index <= size - n`
  (the FFM `get`/`set` also bounds-checks internally as a backstop — see §5). The freed sentinel
  becomes the `freed` boolean.
- `offHeapBuffer` is removed; off-heap segments are backed by `ffm` directly. `getOffHeapBuffer()`
  returns `ffm.asByteBuffer()` (a view onto the same memory, preserving the "holds a reference so
  memory is not released" contract because the `ffm` field keeps the segment reachable, and the
  Arena keeps it alive — see §3).

### 2.2 Method-by-method preservation

- **All scalar `get*`/`put*` (byte/char/short/int/long/float/double, plus NativeEndian/BigEndian
  variants):** preserved 1:1. The `*NativeEndian` methods call `ffm.get/set` with the native-order
  `*_UNALIGNED` layout; the LE/BE public variants keep their existing `LITTLE_ENDIAN ? x :
  reverseBytes(x)` branches unchanged (§4). Float/double keep delegating to int/long bit-conversion.
- **`get(int)`/`put(int,byte)`** and the bulk `get/put(int, byte[], int, int)`: preserved; the
  manual `(offset | length | ...) < 0` range checks stay (they produce the exact tested
  `IndexOutOfBoundsException` messages), then the actual move uses `MemorySegment.copy`.
- **`get(int, ByteBuffer, int)` / `put(int, ByteBuffer, int)`:** preserved. Direct-buffer branch:
  `MemorySegment.copy(ffm, off, MemorySegment.ofBuffer(target), tPos, n)` instead of
  `Unsafe.copyMemory(..., getByteBufferAddress(target)+tPos, ...)`. Heap-array and "no array" branches
  unchanged. The `BufferOverflowException`/`BufferUnderflowException`/`ReadOnlyBufferException`
  throws stay where they are.
- **`copyTo(int, MemorySegment, int, int)`:** preserved — `MemorySegment.copy(this.ffm, off,
  target.ffm, tOff, n)`.
- **`swapBytes(...)`:** preserved — three `MemorySegment.copy` calls (this→temp, other→this,
  temp→other) using a heap FFM view of `tempBuffer`.
- **`get(DataOutput,...)` / `put(DataInput,...)`:** preserved verbatim — already pure Java (they call
  the public scalar accessors for off-heap and `byte[]` IO for heap).
- **`compare`, `equalTo`:** preserved verbatim — already built on the public `getLong*`/`get`
  accessors.
- **`wrap`, `wrap(int,int)`, factories (`wrap(byte[])`, `wrapOffHeapMemory`,
  `allocateHeapMemory`, `allocateOffHeapMemory`):** signatures preserved; bodies re-pointed (§3).
- **`free()`, `isFreed()`, `isOffHeap()`, `size()`, `getArray()`, `getHeapMemory()`:** preserved.

### 2.3 Methods that CANNOT be preserved 1:1 — and why

1. **`copyToUnsafe(int offset, Object target, int targetPointer, int numBytes)`** (MemorySegment:1406)
   and **`copyFromUnsafe(int offset, Object source, int sourcePointer, int numBytes)`**
   (MemorySegment:1427). These take a raw `Object` base + an `int` pointer and call
   `Unsafe.copyMemory(base, ptr, ...)`. **There is no FFM call that copies to/from an arbitrary
   `Object` at an arbitrary integer offset** — FFM copies are segment-to-segment or
   segment-to-array. Resolution:
   - **Audit callers** (`grep -rn "copyToUnsafe\|copyFromUnsafe" --include=*.java`). In practice the
     `Object` passed is always a `byte[]` (or a Fluss segment's heap array). If so, change the
     overload used internally to the typed `copyTo(int, byte[]/MemorySegment, ...)` form, or wrap the
     `byte[]` with `MemorySegment.ofArray((byte[]) target)`.
   - If a non-array `Object` base genuinely exists (it does not in current Fluss source — verify),
     these two methods must be **removed or deprecated**, because faithfully reproducing
     "copy to an arbitrary object field region" is exactly the capability FFM intentionally does not
     expose. Document the break. Since the class is `@Internal`, removal after caller migration is
     acceptable and requires no public-API deprecation cycle.
   This is the **only** signature-level risk; everything else is a body swap.

2. **`getAddress()`** (MemorySegment:232) returns the raw off-heap pointer as a `long`. It can still
   be implemented via `ffm.address()`, so the signature is preserved — **but** the returned value is
   only meaningful while the backing Arena is open, and FFM may hand out addresses that callers must
   not arithmetic-on. Verify no caller does pointer arithmetic with it (grep shows it is internal
   only). Keep it, but note in Javadoc that the address is valid only for the segment's lifetime.

3. **`MemoryUtils.UNSAFE` public field** (MemoryUtils:40) is referenced by `UnsafeUtils`,
   `BinaryArray`, `BinarySegmentUtils`, `MurmurHashUtils`, and a test. The field itself is removed;
   each consumer migrates (§6 Phase B). This is a cross-file change but all consumers are `@Internal`.

---

## 3. Arena lifecycle & ownership for off-heap segments

### 3.1 What we are replacing

Off-heap today: `allocateOffHeapMemory(size)` → `ByteBuffer.allocateDirect(size)` →
`wrapOffHeapMemory` grabs the address via reflection. The direct buffer's native memory is freed
**non-deterministically** by the GC `Cleaner`, or **forcibly** via `ByteBufferUnmapper.unmap(...)`
(reflective `Unsafe.invokeCleaner`). `MemorySegment.free()` only flips the sentinel; it does **not**
release native memory — it relies on GC.

### 3.2 FFM ownership model

Native memory under FFM is owned by an **`Arena`**. Closing the arena frees all its segments
deterministically; accessing a segment after its arena is closed throws
`IllegalStateException` (not a segfault — this is the safety win).

Arena flavors and the choice for Fluss:

| Arena | Frees when | Thread access | Use in Fluss |
|---|---|---|---|
| `Arena.ofConfined()` | `close()` | only the creating thread may access | **NOT** suitable for pooled segments shared across threads (writer thread allocates, network thread copies) → would throw `WrongThreadException`. |
| `Arena.ofShared()` | `close()` (with a global safepoint to fence access) | any thread | **Chosen for pooled off-heap segments** owned by a `MemorySegmentPool`. |
| `Arena.ofAuto()` | GC-driven (like today's Cleaner) | any thread | Fallback for "unmanaged" `allocateOffHeapMemory(size)` calls that have no pool to own them — preserves today's GC-frees-it behavior with zero lifecycle API change. |
| `Arena.global()` | never | any thread | only for process-lifetime singletons; avoid. |

### 3.3 Who owns / closes the Arena — tie to `MemorySegmentPool`

The natural owner is the **pool**, because the pool already owns the segment lifecycle:
`LazyMemorySegmentPool.close()` (line 251) clears `cachePages`, and pages are recycled via
`returnPage`/`returnAll`. We make the Arena a pool-scoped resource:

- Add to `MemorySegmentPool` (the interface already declares `void close()`, line 79) the contract:
  *"closing the pool closes its Arena and frees all native pages."*
- `LazyMemorySegmentPool` gains a `private final Arena arena = Arena.ofShared();` field. Today the
  pool allocates **heap** pages (`MemorySegment.allocateHeapMemory(pageSize)`, line 171); for
  off-heap pools it would allocate via `arena.allocate(pageSize)`. (Note: the writer/server buffer
  pools currently use heap pages, so the immediate behavior is unchanged; the Arena field matters
  once/if off-heap pooling is enabled.)
- `LazyMemorySegmentPool.close()` adds `arena.close()` inside the existing `inLock(lock, ...)` block
  after `cachePages.clear()`. Because the pool is `@ThreadSafe` and the Arena is **shared**, the
  shared-arena close performs the necessary cross-thread fencing. **Pre-close invariant:** all pages
  must have been returned (no live segment access concurrent with `close()`); a shared arena
  `close()` throws if a segment access is in-flight on another thread — which is the desired safety
  net, and matches the existing "Return too more memories" / "closed while allocating" guards.
- For **standalone** off-heap segments created via the static `MemorySegment.allocateOffHeapMemory(int)`
  with no owning pool, back them with `Arena.ofAuto()` so their native memory is reclaimed by GC
  exactly as the `DirectByteBuffer` cleaner does today. This keeps that factory's "fire and forget"
  contract and means **no caller of `allocateOffHeapMemory` needs to change**.

### 3.4 Confined vs shared trade-off (decision)

- **Confined** arenas give the fastest access (no thread-confinement check is amortized differently,
  and the JIT can be more aggressive) but forbid cross-thread access. Fluss segments cross threads
  (produce/fetch hand-off, Arrow encode on one thread, network flush on another), so confined is
  unsafe for the pooled case.
- **Shared** arenas allow the access pattern Fluss needs. The access cost on a shared segment is
  effectively the same as confined for `get`/`set` (the cost is at `close()`, which triggers a
  thread-local handshake / safepoint). Since `close()` happens once per pool lifetime, this is
  negligible.
- **Decision:** shared arena for pool-owned off-heap; auto arena for unmanaged
  `allocateOffHeapMemory`. Never confined for anything that can be returned to a pool.

### 3.5 `ByteBufferUnmapper` retirement

Once off-heap is FFM-backed, the deterministic-free path is `arena.close()`, and
`ByteBufferUnmapper.unmap` (used to force-unmap memory-mapped log files, not pool pages) can be
re-pointed to FFM's `Arena`/`MemorySegment.ofBuffer` mapping path **only if** the mmap path is also
migrated. The mmap of log files is a separate concern (it maps files, not anonymous memory); recommend
**leaving `ByteBufferUnmapper` as-is in Phase 6** (it is not on the `MemorySegment` hot path) and
tracking its FFM migration (`FileChannel.map(..., arena)`) as a follow-up. Note it in §6 Phase E.

---

## 4. On-heap vs off-heap and endianness under FFM

### 4.1 Heap

- `MemorySegment.ofArray(byte[])` produces a **heap** FFM segment whose base is the array; offsets are
  relative. This replaces `BYTE_ARRAY_BASE_OFFSET` entirely (no `arrayBaseOffset`).
- Fluss `wrap(byte[])` / `allocateHeapMemory(int)` set `heapMemory = buffer` and
  `ffm = MemorySegment.ofArray(buffer)`. `getArray()`/`getHeapMemory()` keep returning the array.
- `UnsafeUtils` and `MurmurHashUtils` operate on bare `byte[]` + offset; they migrate to
  `MemorySegment.ofArray(target).get/set(layout, offset)`. To avoid re-wrapping on every call in a
  tight hash loop, `MurmurHashUtils` should wrap once per invocation (the base array is fixed for the
  hash) and reuse the FFM segment across the loop (§5).

### 4.2 Off-heap

- `Arena.allocate(size)` / `arena.allocate(size, alignment)` produces a **native** FFM segment.
- Fluss `allocateOffHeapMemory(int)` → `Arena.ofAuto().allocate(size)`; pool pages →
  `pool.arena.allocate(pageSize)`. `wrapOffHeapMemory(ByteBuffer)` →
  `MemorySegment.ofBuffer(directBuffer)` (preserves the "wrap an existing direct buffer" entry point
  for Kafka/network code that hands us a `ByteBuffer`).
- `isOffHeap()` stays `heapMemory == null`; for off-heap, `heapMemory` is null and `ffm` is the
  native segment.

### 4.3 Endianness — keep the existing two-layer design

The current design is deliberate and the migration must **not** collapse it:

- **Native-endian layer** (`getIntNativeEndian`, `putLongNativeEndian`, …) is the fast path; it reads
  in machine order. Map these to the **native-order** `*_UNALIGNED` layout. On
  `ByteOrder.nativeOrder()` these match. (`ValueLayout.JAVA_INT_UNALIGNED` is native-ordered by
  default; we can also pin it explicitly with `.withOrder(ByteOrder.nativeOrder())`.)
- **LE/BE layer** (`getInt`, `getIntBigEndian`, …) keeps the existing
  `LITTLE_ENDIAN ? native : Integer.reverseBytes(native)` branches. The `LITTLE_ENDIAN` constant
  (MemorySegment:80) stays — it is a `static final boolean`, so the JIT still folds the dead branch,
  exactly as the current Javadoc (lines 76–79) intends.

**Why keep manual `reverseBytes` instead of using BE/LE `ValueLayout` constants directly?** Two
reasons: (1) it is byte-for-byte the existing behavior, minimizing audit surface; (2) `reverseBytes`
is a single intrinsic and on a same-endian read it compiles away, so there is no perf loss versus
selecting an LE/BE layout. Switching to dedicated `JAVA_INT.withOrder(LITTLE_ENDIAN)` layouts is a
*possible* later cleanup but is **not** part of this migration (it changes the code shape the perf
gate measured). The compare/equalTo paths (`getLongBigEndian`, `getLongNativeEndian`) inherit this
automatically.

### 4.4 `BinaryArray` typed-array writes

`BinaryArray` (lines 618–619) does `UNSAFE.putInt(data, BYTE_ARRAY_BASE_OFFSET, length)` then
`UNSAFE.copyMemory(...)` to lay out a header + payload in a `byte[]`. Migrate to
`MemorySegment.ofArray(data).set(JAVA_INT_UNALIGNED, 0, length)` and `MemorySegment.copy`. The
per-type array base offsets (lines 60–65) are only needed for the typed bulk-copy variants; under FFM
those become `MemorySegment.copy(MemorySegment.ofArray(srcTypedArray), srcOff,
MemorySegment.ofArray(data), dstOff, bytes)` and the per-type offset constants are deleted.

---

## 5. Performance pitfalls and how to avoid regressing vs `Unsafe`

The JMH gate is `fluss-jmh/.../MemorySegmentBenchmark.java` (heap+offheap × sequential int/long
read/write + bulk put/get). Its own Javadoc warns: *"a naive FFM port can be slower than Unsafe
without confined arenas."* Rules to hold parity:

1. **Use `*_UNALIGNED` layouts, not aligned ones.** Byte-addressed access at arbitrary `index` is
   unaligned by construction. Aligned `ValueLayout.JAVA_INT` throws on misaligned offsets *and*
   the alignment check itself is overhead. `JAVA_INT_UNALIGNED` is the correct, fast choice. This is
   the single most common FFM perf/correctness mistake.

2. **Do not allocate FFM segments in the hot loop.** `MemorySegment.ofArray(byte[])` is cheap but not
   free; for heap segments, build the `ffm` view **once in the constructor** and store it in the
   field (§2.1). `UnsafeUtils`/`MurmurHashUtils`, which take a bare `byte[]` per call, should wrap
   once per method invocation and reuse across the loop — never per element.

3. **Lean on bounds-check elision.** FFM `get`/`set` bounds-check internally. Fluss also does its own
   `index >= 0 && index <= size - n` check. Two checks would regress. Mitigation: keep Fluss's own
   collapsed range check (it produces the tested exception messages) and rely on the JIT to **elide
   the redundant FFM check** when the offset is provably in-bounds — which it can, because the FFM
   segment's size is a `final` known at the access site and the preceding Fluss check dominates it.
   Validate with the JMH gate; if the double-check shows up, the Fluss-level check is the one to keep
   (for message fidelity) and we accept the FFM check as the JIT-elided backstop. Do **not** disable
   FFM bounds checks (there is no supported way, and that is the safety we are buying).

4. **Shared arena, not confined, for cross-thread pooled memory** (§3.4). Confined would throw at
   runtime; shared has negligible per-access cost (the cost is at `close()`).

5. **Warm up.** FFM access is intrinsified by C2 after warmup; cold/interpreted FFM is markedly
   slower than cold Unsafe. The benchmark already uses `@Warmup(iterations = 5) @Fork(1)`. Keep it.

6. **`MemorySegment.copy` for bulk** — it lowers to the same `memmove`/`memcpy` intrinsic as
   `Unsafe.copyMemory`, so `bulkPutThenGet` should be at parity. Prefer the segment/array overloads
   over manual loops.

7. **Avoid `asByteBuffer()`/`asSlice()` churn** on hot paths; only `wrap(int,int)`/`getOffHeapBuffer`
   need a `ByteBuffer` view and those are not per-element.

**Gate:** record the current Unsafe baseline numbers from `MemorySegmentBenchmark` first (all 5
benchmarks × {heap, offheap}). After each migration phase that touches `MemorySegment`, re-run and
require **no statistically significant regression** (overlapping error bars) on any of the 10
data points. Treat a >5% median regression on any hot accessor as a blocker.

---

## 6. Phased, testable migration order

Each phase compiles and passes the full `fluss-common` test suite (`CrossMemorySegmentTypeTest`,
binary-row tests, hash tests) before the next begins. JDK 25 is required (FFM finalized in 22, but
Fluss targets 25 per `0023`). The source must still **compile** under the project's Java-8 source
level only where FFM is *not* used — FFM types are JDK 25; therefore the `memory`/`utils` files that
adopt FFM move to a Java-25 source set or are guarded by the multi-release/`0023` migration. **This
migration is dependent on `0023-java25-migration.md` landing first** (it raises the source level).

### Phase A — Baseline & scaffolding (no behavior change)
1. Run `MemorySegmentBenchmark` on the current Unsafe code; archive the 10 numbers as the gate.
2. Add a Java-25 FFM smoke ITCase: allocate heap + shared-arena off-heap FFM segments, round-trip
   int/long/byte at unaligned offsets, assert values, close arena, assert post-close access throws
   `IllegalStateException`. This proves the platform before touching production code.

### Phase B — Migrate the leaf helpers (smallest blast radius)
3. `UnsafeUtils` → FFM (`MemorySegment.ofArray` + `JAVA_*_UNALIGNED`). Rename to `ByteArrayUtils` or
   keep the name with a note; it no longer touches Unsafe. Update its callers.
4. `MurmurHashUtils` → FFM, wrapping the base array once per hash call (§5.2).
5. `BinaryArray` / `BinarySegmentUtils` → remove `arrayBaseOffset` constants and `UNSAFE` import;
   use `MemorySegment.ofArray` + `JAVA_INT_UNALIGNED` + `MemorySegment.copy`.
6. Run binary-row and hash unit tests. No `MemorySegment.java` change yet.

### Phase C — Migrate `MemoryUtils`
7. Delete `getByteBufferAddress` + `BUFFER_ADDRESS_FIELD_OFFSET` + the reflective `getUnsafe()` +
   the public `UNSAFE` field (only after Phases B and D consumers are gone — sequence carefully; may
   reorder C after D). Keep `NATIVE_BYTE_ORDER`.

### Phase D — Migrate `MemorySegment` core (the heart)
8. Introduce the new fields (§2.1): `ffm`, keep `heapMemory`, `size`, `freed`.
9. Re-point factories: `wrap(byte[])`, `wrapOffHeapMemory(ByteBuffer)` (→ `ofBuffer`),
   `allocateHeapMemory` (→ `ofArray`), `allocateOffHeapMemory` (→ `Arena.ofAuto().allocate`).
10. Re-point all scalar `get*`/`put*` to `ffm.get/set` with native-order `*_UNALIGNED` layouts,
    preserving the LE/BE `reverseBytes` branches and all exception messages.
11. Re-point bulk methods (`get/put byte[]`, `get/put ByteBuffer`, `copyTo`, `swapBytes`) to
    `MemorySegment.copy`.
12. Resolve `copyToUnsafe`/`copyFromUnsafe` (§2.3 item 1): audit callers, switch to typed copies, or
    remove if no non-array base exists.
13. Replace the `address > addressLimit` freed-sentinel logic with the `freed` boolean throughout;
    `free()` sets it, every accessor checks it (preserve "segment has been freed" messages).
14. Re-run `MemorySegmentBenchmark` → compare to Phase A gate. Re-run `CrossMemorySegmentTypeTest`
    and all `fluss-common` memory/row tests.

### Phase E — Pool ownership & cleanup
15. Add the `Arena` field + `close()` semantics to `LazyMemorySegmentPool` (§3.3). (Heap pools keep
    heap pages; the arena is wired in for the off-heap pooling path.)
16. Decide `ByteBufferUnmapper` fate: leave as-is for now (mmap log files, off hot path) with a
    follow-up ticket for `FileChannel.map(..., arena)` (§3.5).
17. Coordinate with `0023`: remove the `--add-opens java.base/java.nio…` /
    `jdk.internal.ref` / `sun.nio.ch` flags from the daemon scripts and Maven `argLine` **only after**
    confirming a clean compile+test with each removed (the Arrow flags stay until the Arrow bump).

### Phase F — Verification
18. Full `mvn verify` on `fluss-common`, `fluss-server`, Kafka + Arrow codec ITCases.
19. JDK 25 run with `-Djava.lang.foreign.*` warnings surfaced; confirm **zero** `Unsafe` reflection
    warnings remain from Fluss-owned code (Arrow shaded excepted).
20. Sign-off: JMH gate green (no >5% regression), all tests green.

---

## 7. Risk summary

| Risk | Severity | Mitigation |
|---|---|---|
| FFM access slower than Unsafe if misused | High | `*_UNALIGNED` layouts, cache `ffm` view, shared arena, JMH gate (§5). |
| `copyToUnsafe`/`copyFromUnsafe` have no FFM equivalent for non-array bases | Medium | Audit shows only `byte[]` bases today; switch to typed copy or remove (§2.3). |
| Double bounds-checking (Fluss + FFM) | Medium | Rely on JIT elision; keep Fluss check for message fidelity; verify via JMH (§5.3). |
| Arena lifetime vs returned pool pages | Medium | Pool owns shared arena; `close()` after all pages returned; shared-arena close fences access (§3.3). |
| Depends on JDK 25 source level (`0023`) | High (sequencing) | Land `0023` first; FFM files in the Java-25 source set. |
| Shaded Arrow still uses Unsafe | Low (out of scope) | Arrow version bump per `0023`, not this doc (§1.4). |
| Endianness regression in compare/sort | High (correctness) | Keep the exact native/LE/BE two-layer design and `reverseBytes` branches (§4.3). |
