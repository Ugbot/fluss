## Scope & objective

Roadmap `dev-docs/design/0020-execution-order-and-roadmap.md` Phase 2 (lines 78–83): flip the source/target level to Java 25, delete the `java8` profile and the `compile-on-jdk8` CI job, drop the `--add-exports`/`--add-opens` crutches that strong-encapsulation no longer needs, and bump deps unlocked by leaving Java 8 (Caffeine 3.x etc.). **No behaviour change** — exit criterion is "builds/tests on JDK 25; baseline delta recorded."

This is a read-only deep dive. Below are exact file-level edits, the dependency bumps unlocked, the Checkstyle situation, and the risk flags (shaded Arrow / Netty / ZooKeeper / FRocksDB JNI).

---

## 1. Root `pom.xml` edits — `/Users/bengamble/fluss/pom.xml`

### 1.1 Bump the Java level property (line 90)

```xml
<!-- line 90, current -->
<target.java.version>11</target.java.version>
<!-- change to -->
<target.java.version>25</target.java.version>
```

This single property feeds `maven.compiler.source`/`maven.compiler.target` (lines 91–92), the `maven-compiler-plugin` `<source>`/`<target>` in both the active `<build>` (lines 581–582) and `<pluginManagement>` (lines 984–985), and the `maven-enforcer-plugin` `requireJavaVersion` rule (line 800). One change propagates to all of them.

> Note: `maven.compiler.source`/`target` and `<release>8</release>` in the javadoc plugin are independent of each other; see 1.6 for the javadoc one.

### 1.2 Delete the `skip.on.java8` property (line 97)

```xml
<!-- line 97, delete -->
<skip.on.java8>false</skip.on.java8>
```

Once the `java8` profile is gone this property is only ever `false`. But it is referenced by **5 sub-module poms** (see §3). Deleting it from the root requires editing those modules too, OR — lower-risk alternative — keep the property pinned to `false` permanently and only delete the profile. **Recommended: delete the property and clean up the 5 consumers** (§3) so no dead config remains; this matches the roadmap intent of removing crutches.

### 1.3 Delete the entire `java8` profile (lines 461–484)

```xml
<!-- lines 461-484, delete the whole <profile> block -->
<profile>
    <id>java8</id>
    <properties>
        <target.java.version>1.8</target.java.version>
        <skip.on.java8>true</skip.on.java8>
    </properties>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>${target.java.version}</source>
                    <target>${target.java.version}</target>
                    <useIncrementalCompilation>false</useIncrementalCompilation>
                    <compilerArgs>
                        <arg>-Xpkginfo:always</arg>
                    </compilerArgs>
                </configuration>
            </plugin>
        </plugins>
    </build>
</profile>
```

### 1.4 `maven-compiler-plugin` `--add-exports` args (lines 585–593) — which stay, which can go

The active `<build>` compiler plugin passes these `compilerArgs`:

```xml
<arg>--add-exports=java.base/sun.net.util=ALL-UNNAMED</arg>          <!-- line 586 -->
<arg>--add-exports=java.management/sun.management=ALL-UNNAMED</arg>  <!-- line 587 -->
<arg>--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED</arg>       <!-- line 588 -->
<arg>--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED</arg> <!-- line 589 -->
<arg>--add-exports=java.base/sun.nio.ch=ALL-UNNAMED</arg>            <!-- line 590 -->
<arg>-Xpkginfo:always</arg>                                          <!-- line 592 -->
```

These are **compile-time** `--add-exports` (needed because some source references JDK-internal `sun.*` symbols). They are NOT removed "for free" by Java 25 — `--add-exports` controls *compile/access* to internal packages, and strong encapsulation in Java 16+/25 makes them *more* necessary, not less, for any code still touching `sun.*`. **Recommendation: keep them initially**, then remove each one only after confirming (via a clean compile with it removed) that no source still depends on that internal package. The roadmap line 81 ("drop `--add-exports`/`--add-opens` crutches … as strong-encapsulation/FFM allow") explicitly means *as the migration to FFM removes the underlying `sun.*` usage* (Phase 6 migrates `MemorySegment` off `sun.misc.Unsafe`). So:

- `sun.nio.ch`, `sun.net.util`, `sun.management`, `sun.rmi.registry`, `sun.security.krb5` — **KEEP for Phase 2**; revisit per-arg after Phase 6 FFM work removes the `sun.misc.Unsafe`/internal NIO usage. Verify each by deleting it and running `mvn -pl <module> compile`.
- `-Xpkginfo:always` (MCOMPILER-205 workaround) and `useIncrementalCompilation=false` (MCOMPILER-209) — **KEEP**; unrelated to Java version.

### 1.5 **Bump `maven-compiler-plugin` 3.8.0 → 3.13.0 (REQUIRED, blocker)**

Version `3.8.0` (lines 579, 981) predates JDK 25 and does not understand `<release>25</release>` / `--release 25`; it will fail or mis-target. Bump to **3.13.0+** in both the `<build>` plugin (line 579) and `<pluginManagement>` (line 981):

```xml
<artifactId>maven-compiler-plugin</artifactId>
<version>3.13.0</version>
```

Also consider switching `<source>`/`<target>` to `<release>${target.java.version}</release>` — cleaner and the canonical form for 9+. Optional but recommended.

### 1.6 javadoc plugin `<release>8</release>` (line 1093)

```xml
<release>8</release>   <!-- line 1093 -->
```

Bump to `25` (or `${target.java.version}`). The adjacent `--ignore-source-errors` JOption (line 1098, comment line 1097 "accepted by JDK 8 but not by JDK 11") was a JDK-8↔11 javadoc workaround — re-evaluate; likely removable once on 25.

### 1.7 Pin Surefire/Shade plugins — version-compatibility blockers (RISK)

These pinned versions predate JDK 25 and are likely to break on it:
- `maven-surefire-plugin` **3.0.0-M5** (lines 712, 1113) — old milestone; JDK 25 forked-JVM/byte-buddy issues. **Bump to 3.5.x** (e.g. 3.5.2).
- `maven-shade-plugin` **3.4.1** (line 1120) — shading on JDK 25 class-file major version 69 needs ASM ≥ 9.7; **bump to 3.6.0**.
- `maven-jar-plugin` **2.4** (line 563) — ancient; bump to 3.4.x for JDK 25 manifest handling.

These are not strictly "Phase 2 source-level" but are hard prerequisites for the build to run on JDK 25; flag them as part of the same PR.

### 1.8 `extraJavaTestArgs` `--add-opens` (lines 160–178) — test-runtime, KEEP (mostly)

The Surefire `argLine` (line 743) injects `${extraJavaTestArgs}`, which is 14 `--add-opens` plus `-XX:+IgnoreUnrecognizedVMOptions`, `-Djdk.reflect.useDirectMethodHandle=false`, `-Dio.netty.tryReflectionSetAccessible=true`. These are **runtime reflective-access** opens needed by Arrow/Netty/test reflection, NOT compile crutches. On Java 25 strong encapsulation is fully enforced, so removing these will *break* tests that reflect into `java.base`. **KEEP the block.** Targeted review:
- `--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED` and `sun.nio.ch` (lines 171–172) — required by Arrow off-heap/`MemoryUtil`. KEEP until Phase 6 FFM migration.
- `-Djdk.reflect.useDirectMethodHandle=false` (line 176) — legacy reflection flag; verify still recognised on 25, otherwise the `IgnoreUnrecognizedVMOptions` swallows it harmlessly.
- The `--add-opens` for `java.util.concurrent.atomic`, `java.lang.invoke`, etc. — KEEP; many are mockito/byte-buddy + Netty needs.

Do not delete in Phase 2; this is runtime, behaviour-preserving, and the roadmap's "no behaviour change" rule applies.

---

## 2. CI YAML edits

### 2.1 `/Users/bengamble/fluss/.github/workflows/ci.yaml`

Delete the entire `compile-on-jdk8` job (lines 40–53) and set the reusable build to JDK 25 (line 59):

```yaml
# DELETE lines 40-53 (the whole compile-on-jdk8 job)
jobs:
  compile-on-jdk8:
    name: "Compile Java 8"
    ...
    run: |
      mvn -T 1C -B clean install -DskipTests -Pjava8

# KEEP build-on-jdk11 but rename + retarget:
  build-on-jdk25:
    name: "Java 25"
    uses: ./.github/workflows/ci-template.yaml
    with:
        java-version: "25"
```

### 2.2 `/Users/bengamble/fluss/.github/workflows/nightly.yaml`

The nightly runs the Java 8 build (lines 28–34). Delete or retarget — Java 8 nightly is meaningless once source is 25:

```yaml
# lines 28-34, current
jobs:
  build-on-jdk8:
    name: "Java 8"
    uses: ./.github/workflows/ci-template.yaml
    with:
      java-version: "8"
      maven-parameters: "-Pjava8"
```
**Replace** with a nightly on the *next* JDK EA (e.g. 26-ea) or simply delete the job. Recommended: retarget to `java-version: "26-ea"` (forward-looking smoke test) since the main matrix already covers 25.

### 2.3 `/Users/bengamble/fluss/.github/workflows/license-check.yml` (line 40)

Uses `java-version: 11`. Bump to `25` for consistency (it only builds + runs the license checker; low risk).

### 2.4 `/Users/bengamble/fluss/.github/workflows/ci-template.yaml` — no edit required

It is parametrised by `inputs.java-version`; setup-java@v5 + temurin support 25. No change needed beyond callers passing `"25"`. `actions/setup-java@v5` distribution `temurin` must have a 25 build available (it does as of late 2025) — verify in the run.

> The `-Pjava8` profile reference only appears in `ci.yaml` (deleted) and `nightly.yaml` (retargeted). No other workflow passes it.

---

## 3. Sub-module `skip.on.java8` consumers (must be cleaned up if §1.2 deletes the property)

These 5 modules reference `${skip.on.java8}` to skip main+test compilation under the java8 profile (because they require Java 11+ already — Flink 2.2, Spark 3.x, Iceberg, InfluxDB metrics):

| File | Lines |
|------|-------|
| `fluss-flink/fluss-flink-2.2/pom.xml` | 198, 200, 218, 235 |
| `fluss-lake/fluss-lake-iceberg/pom.xml` | 260, 262, 279, 296 |
| `fluss-metrics/fluss-metrics-influxdb/pom.xml` | 79, 81 |
| `fluss-spark/fluss-spark-3.5/pom.xml` | 59, 61, 79, 96 |
| `fluss-spark/fluss-spark-3.4/pom.xml` | 59, 61, 79, 96 |

Each uses the pattern:
```xml
<skipMain>${skip.on.java8}</skipMain>
<skip>${skip.on.java8}</skip>
```
on `maven-compiler-plugin` and the module's `maven-surefire-plugin` executions. With Java 8 gone these guards are always-false dead config. **Action:** remove the `<skipMain>`/`<skip>${skip.on.java8}</skip>` lines from these 5 poms (the modules now always compile). If you prefer minimal diff, instead keep a root property `<skip.on.java8>false</skip.on.java8>` so these references still resolve — but that leaves a misleadingly-named permanent property. Recommend full cleanup.

---

## 4. Dependency version bumps unlocked

### 4.1 Caffeine (the explicit roadmap target) — `/Users/bengamble/fluss/fluss-server/pom.xml`

```xml
<!-- line 49-50, current -->
<!-- 2.9.3 is the latest version that supports JDK 8 -->
<caffeine.version>2.9.3</caffeine.version>
<!-- change to (Caffeine 3.x requires Java 11+; on Java 25 use latest 3.x) -->
<caffeine.version>3.2.0</caffeine.version>
```
Remove the now-false "latest version that supports JDK 8" comment. Caffeine 3.x is a drop-in API-compatible upgrade (same `com.github.ben-manes.caffeine.cache.*` API); only the JDK floor changed. This is the **only** dep in the tree with an explicit Java-8 pin comment besides the s3 filter note (§4.3).

### 4.2 Shaded deps — bump only via `fluss-shaded` releases (see §5 RISK)

The roadmap (line 82) lists Arrow, Netty io_uring, ZK/curator. These are **shaded** (`org.apache.fluss:fluss-shaded-*`, version `${x.version}-${fluss.shaded.version}` where `fluss.shaded.version=1.0-incubating`). The version *properties* in root pom are:
- `arrow.version` 15.0.0 (line 106)
- `netty.version` 4.1.104.Final (line 105)
- `zookeeper.version` 3.8.3 (line 103), `curator.version` 5.4.0 (line 104)

Bumping these requires a **new `fluss-shaded` release** that re-shades the upgraded upstream; you cannot just bump the property here without a matching shaded artifact. Treat as a separate, coordinated change (out of the pure-pom Phase-2 PR). Targets when shaded artifacts exist:
- Arrow → 17.x/18.x (better JDK 21/25 + FFM memory support; removes some `--add-opens` need long-term).
- Netty → 4.1.115+ (io_uring transport stability on modern Linux/JDK).
- ZooKeeper → 3.9.x + Curator 5.7.x (JDK 21/25 tested).

### 4.3 Non-blocking / informational

- `fluss-filesystems/fluss-fs-s3/pom.xml` line ~370: comment "Filter must be removed … when discontinuing support for Java 8" on the `aws-java-sdk-s3` `XmlResponsesSaxParser` exclude. This is AWS SDK **v1**, which Phase 5 migrates to v2 — not a Phase 2 action, but the comment's precondition (dropping Java 8) is now met. Leave for Phase 5.
- `frocksdb.version` 6.20.3-ververica-2.0 (line 120) — see §5 RISK (JNI). No bump assumed for Phase 2.

---

## 5. Checkstyle — forbidden-Java-9+-feature rules

**Key finding: there are NO Checkstyle rules to relax.** I inspected `tools/maven/checkstyle.xml` end-to-end. The "FORBIDDEN Java 9+ features" list in `CLAUDE.md` (`var`, `List.of()`, `Optional.isEmpty()`, `String.strip()`, `Stream.toList()`, switch expressions, records, etc.) is **documentation/convention only — none of it is enforced by Checkstyle**. There are no `Regexp`/`IllegalToken`/`MatchXpath` modules matching `var`, `List.of`, `isEmpty`, `toList`, `strip`, `isBlank`, `switch`, `sealed`, or `record` (grep returned nothing).

The Checkstyle `Regexp`/`IllegalImport` rules that DO exist are unrelated to Java version and **must stay**: shaded-import bans (jackson/guava, lines 220–231), AssertJ-over-JUnit (lines 172–183), `@Timeout` ban (line 184), `Boolean.getBoolean`/`Integer.getInteger`/`Long.getLong` (lines 96–111), commons-lang3 substitutions, `Throwables.propagate`, TODO-username, trailing-whitespace, `ArrayTypeStyle`, `RedundantModifier`.

**Action for Phase 2:** Update **`CLAUDE.md` Section 1 "Java Version Compatibility"** — strike the "Source level: Java 8" line and the entire "FORBIDDEN Java 9+ features" block (the `var`/`List.of`/`Optional.isEmpty`/switch-expr/records/sealed/pattern-matching prohibitions). On Java 25 these become *allowed* and idiomatic. No checkstyle.xml edit is required because the bans were never codified there. (Optionally, add new positive style rules later, but that's not part of "relaxing".)

Also note `googleJavaFormat` is pinned to **1.15.0** (pom line 1033) with AOSP style — verify it formats Java 25 syntax (records, switch expressions, sealed) correctly; if you start using those features, bump google-java-format to ≥ 1.22 (still AOSP). Not needed if code stays syntactically conservative in Phase 2.

---

## 6. Runtime launch scripts (`fluss-dist`) — behaviour to preserve, not Phase-2 edits

These already gate Java-17+ runtime flags and will keep working on 25; **no edits required for Phase 2**, but document them so they aren't mistaken for crutches to remove:
- `fluss-dist/src/main/resources/bin/fluss-daemon.sh:130-134` and `fluss-console.sh:105-108` — conditionally add `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` when JDK ≥ 17 (Arrow off-heap). **KEEP** — still required on 25 until Arrow/FFM migration (Phase 6).
- `fluss-dist/src/main/resources/bin/config.sh:262-266` — adds `-Djava.security.manager=allow` when `java.specification.version > 17`. The SecurityManager is **removed/terminally-disabled in JDK 25** (JEP 486). On 25 `-Djava.security.manager=allow` is at best a no-op and may warn; `-XX:+IgnoreUnrecognizedVMOptions` is *not* applied to `-D` system properties, so this could surface a warning or error. **FLAG:** verify on JDK 25 whether `-Djava.security.manager=allow` is still accepted; if it errors, this `config.sh` block must be removed/guarded to `== 17` only. This is the one launch-script change Phase 2 may force.

---

## 7. Risk register (shaded-dep & JNI Java 25 compatibility)

| Component | Pinned version | Java 25 risk | Mitigation |
|-----------|---------------|--------------|------------|
| **Arrow (shaded)** | 15.0.0 | Arrow 15 predates official JDK 21/25 testing; relies on `--add-opens java.base/java.nio…memory.core` and `jdk.internal.ref`. Strong encapsulation on 25 enforces these. `MemoryUtil` uses `sun.misc.Unsafe`/`DirectByteBuffer` cleaner reflection — may break. | Keep the runtime `--add-opens` (daemon scripts + `extraJavaTestArgs`). Validate Arrow alloc/free under JDK 25 in a smoke ITCase. Plan shaded-Arrow bump to 17/18 (better JDK 25 + FFM). **Highest risk item.** |
| **Netty (shaded)** | 4.1.104.Final | 4.1.104 predates JDK 25; `PlatformDependent` reflective cleaner + `sun.misc.Unsafe` access; io_uring transport. `tryReflectionSetAccessible=true` (test argLine) needed. | Keep `-Dio.netty.tryReflectionSetAccessible=true` + add-opens. Bump shaded Netty to ≥ 4.1.115 which has JDK 21/25 fixes. Verify TLS/SSL (`SslContext`) handshakes on 25's TLS stack. |
| **ZooKeeper 3.8.3 + Curator 5.4.0 (shaded ZK)** | 3.8.3 / 5.4.0 | ZK 3.8 is tested to JDK 17; 3.8.x generally runs on 21/25 but not certified. Curator 5.4 is old. | Run the ZK registration ITCases on 25 (TabletServer/CoordinatorServer register loops). Bump shaded ZK→3.9.x + Curator→5.7.x. |
| **FRocksDB JNI** | `com.ververica:frocksdbjni:6.20.3-ververica-2.0` | **JNI native lib** — pure-native `.so`/`.dylib`; the JVM major version generally does not affect a loaded JNI lib, BUT the Java-side binding may use removed/encapsulated APIs and the native lib must support the host glibc. Low-but-nonzero risk; this is a Ververica-custom build with no newer JDK testing. | Run RocksDB-backed KV ITCases (`KvTablet`, snapshot) on JDK 25. If the JNI loader or finalizer path breaks, no easy bump exists (custom artifact) — escalate; may need a rebuilt frocksdbjni. **Second-highest risk** because it's a custom, un-bumpable artifact. |
| **maven-compiler/surefire/shade plugins** | 3.8.0 / 3.0.0-M5 / 3.4.1 | Will not understand class major 69 / `--release 25`; forked-test byte-buddy + ASM too old. | §1.5 / §1.7 bumps. **Hard build blockers — must land in the same PR.** |
| **google-java-format 1.15.0** | 1.15.0 | Cannot format Java 21/25 syntax (records/switch-expr) if such syntax is introduced. | Keep code conservative in Phase 2, or bump to ≥ 1.22. |
| **SecurityManager (`-Djava.security.manager=allow`)** | config.sh | Removed in JDK 25 (JEP 486). | §6 — verify/guard the `config.sh` block. |

---

## 8. Suggested PR decomposition (keeps each step green & benchmark-gated per roadmap)

1. **PR-A (build plumbing, no source change):** bump maven-compiler→3.13.0, surefire→3.5.x, shade→3.6.0, jar→3.4.x. Still on Java 11. Confirms tooling works before flipping the level.
2. **PR-B (Java 25 flip):** root `pom.xml` line 90 →25; delete `java8` profile (461–484) + `skip.on.java8` property (97) + the 5 sub-module consumers (§3); javadoc `<release>` (1093) →25; bump Caffeine →3.2.0 (`fluss-server/pom.xml`); CI: delete `compile-on-jdk8`, retarget `ci.yaml`/`nightly.yaml`/`license-check.yml` to 25; guard/remove `config.sh` SecurityManager block; update `CLAUDE.md` §1. Keep all `--add-exports`/`--add-opens`.
3. **PR-C (separate, coordinated):** shaded-dep bumps (Arrow/Netty/ZK+Curator) once new `fluss-shaded` artifacts exist; then revisit per-arg `--add-exports`/`--add-opens` removal — guarded by re-running Phase 1 JMH benchmarks (roadmap exit criterion "baseline delta recorded").

---

## 9. Files touched (absolute paths)

- `/Users/bengamble/fluss/pom.xml` — lines 90, 97, 461–484, 563, 579, 712, 981, 1093, 1098, 1113, 1120; review 585–593 & 160–178.
- `/Users/bengamble/fluss/fluss-server/pom.xml` — lines 49–50 (Caffeine).
- `/Users/bengamble/fluss/.github/workflows/ci.yaml` — lines 40–59.
- `/Users/bengamble/fluss/.github/workflows/nightly.yaml` — lines 28–34.
- `/Users/bengamble/fluss/.github/workflows/license-check.yml` — line 40.
- `/Users/bengamble/fluss/fluss-flink/fluss-flink-2.2/pom.xml` — 198, 200, 218, 235.
- `/Users/bengamble/fluss/fluss-lake/fluss-lake-iceberg/pom.xml` — 260, 262, 279, 296.
- `/Users/bengamble/fluss/fluss-metrics/fluss-metrics-influxdb/pom.xml` — 79, 81.
- `/Users/bengamble/fluss/fluss-spark/fluss-spark-3.5/pom.xml` — 59, 61, 79, 96.
- `/Users/bengamble/fluss/fluss-spark/fluss-spark-3.4/pom.xml` — 59, 61, 79, 96.
- `/Users/bengamble/fluss/fluss-dist/src/main/resources/bin/config.sh` — 262–266 (SecurityManager guard).
- `/Users/bengamble/fluss/CLAUDE.md` — Section 1 "Java Version Compatibility" (doc only).
- **No change to** `/Users/bengamble/fluss/tools/maven/checkstyle.xml` (no Java-9-feature rules exist) or `/Users/bengamble/fluss/.github/workflows/ci-template.yaml` (already parametrised).
