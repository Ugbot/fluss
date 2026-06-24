## Dependency Diet — Roadmap 0020 Phase 5 (concrete spec)

Read-only audit of every POM that pulls Hadoop / Flink / AWS-SDK-v1 / Scala / Spark, against `dev-docs/design/0020-execution-order-and-roadmap.md` lines 120–136 and 194–208. All paths are absolute. Worktree copies under `.claude/worktrees/**` are ignored — only the live tree is specified.

### Audit summary (where the heavy deps actually live)

| Dep | POM(s) | Scope today |
|---|---|---|
| `hadoop-common` / `hadoop-hdfs-client` | `/Users/bengamble/fluss/fluss-lake/fluss-lake-iceberg/pom.xml:139-193` | `provided` (main) + `hadoop-mapreduce-client-core` 2.8.5 `test` |
| `hadoop-common` / `hadoop-hdfs-client` | `/Users/bengamble/fluss/fluss-lake/fluss-lake-paimon/pom.xml:107-127` | `provided` (main) |
| `hadoop-common` + `hadoop-aws` + AWS SDK **v1** (`com.amazonaws:aws-java-sdk-*` 1.12.319) | `/Users/bengamble/fluss/fluss-filesystems/fluss-fs-s3/pom.xml:36,49-235` | `compile`, bundled/shaded into the fat fs jar |
| `hadoop-aliyun` / `hadoop-huaweicloud` / `hadoop-azure` / `hadoop-cos` / GCS `hadoop3` + `fluss-fs-hadoop-shaded` | `/Users/bengamble/fluss/fluss-filesystems/fluss-fs-{oss,obs,azure,cos,gs}/pom.xml` | `compile`, bundled |
| Flink (`flink-java`, `flink-streaming-java` 1.20.3) | `/Users/bengamble/fluss/fluss-flink/fluss-flink-tiering/pom.xml:48-57` | `compile` — tiering runtime |
| Flink (`flink-core`, `flink-table-*`, `iceberg-flink-1.20`) | `/Users/bengamble/fluss/fluss-lake/fluss-lake-iceberg/pom.xml:195-250`, `fluss-lake-paimon/pom.xml:173-235` | `test` only |
| Scala 2.12/2.13 + Spark 3.4/3.5 | `/Users/bengamble/fluss/fluss-spark/**` (root props `pom.xml:112-117`) | full module tree |
| Caffeine **2.9.3** | `/Users/bengamble/fluss/fluss-server/pom.xml:50,74-76` | `compile` |

Key finding: `fluss-lake-iceberg` already loads Hadoop **reflectively** via `DynClasses` in `/Users/bengamble/fluss/fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/conf/IcebergConfiguration.java:79-94` and returns `null` when Hadoop is absent (`buildIcebergCatalog(name, props, null)` is legal). So Hadoop on the Iceberg path is already optional at runtime — Phase 5 just removes it from the dependency set and supplies `S3FileIO` instead of `HadoopFileIO`.

---

## (1) Drop hadoop-common / hadoop-aws from the tiering + Iceberg path → iceberg-aws S3FileIO + AWS SDK v2

### 1a. Root `pom.xml` — add AWS SDK v2 BOM + iceberg-aws to `<dependencyManagement>`

File: `/Users/bengamble/fluss/pom.xml`. Add property next to the existing versions (after line 108 `iceberg.version`):

```xml
<aws.sdk2.version>2.28.16</aws.sdk2.version>
```

Add to `<dependencyManagement><dependencies>` (anywhere in the block around lines 250–460), importing the AWS v2 BOM so every module gets a single, converged v2 line:

```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>bom</artifactId>
    <version>${aws.sdk2.version}</version>
    <type>pom</type>
    <scope>import</scope>
</dependency>
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-aws</artifactId>
    <version>${iceberg.version}</version>
</dependency>
```

### 1b. `fluss-lake-iceberg/pom.xml` — remove Hadoop main deps, add iceberg-aws + S3 client

File: `/Users/bengamble/fluss/fluss-lake/fluss-lake-iceberg/pom.xml`.

**REMOVE** the two `provided` Hadoop main dependencies (lines 133–193): the `hadoop-hdfs-client` block and the entire `hadoop-common` block (with its exclusion list). **KEEP** `hadoop-mapreduce-client-core` 2.8.5 at lines 120–131 *only if* an ORC test still needs it — see note below; preferred is to drop it too and rely on `iceberg-data`.

**ADD** after the `iceberg-bundled-guava` block (line 76):

```xml
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-aws</artifactId>
    <version>${iceberg.version}</version>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>s3</artifactId>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>sts</artifactId>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>apache-client</artifactId>
</dependency>
```

(Versions come from the BOM in 1a — no `<version>` on the `software.amazon.awssdk:*` lines.)

### 1c. Source change to select S3FileIO (one-line config default)

The catalog is built in `/Users/bengamble/fluss/fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/utils/IcebergCatalogUtils.java:34-38`. No structural change needed — `buildIcebergCatalog` already passes `icebergProps`. Phase 5 sets `io-impl=org.apache.iceberg.aws.s3.S3FileIO` as the default in that prop map when no `io-impl` is supplied and the warehouse URI is `s3://`/`s3a://`, instead of letting Iceberg fall back to `HadoopFileIO`. `IcebergConfiguration.from(...)` (line 38) keeps returning `null` when Hadoop is off the classpath — that path is already correct and needs no edit. `HadoopUtils.java` and `IcebergConfiguration.java` stay but become dormant (only used for HDFS catalogs that still ship Hadoop via the FS plugin).

### 1d. Note: `fluss-lake-paimon` Hadoop stays (genuinely needs it)

File: `/Users/bengamble/fluss/fluss-lake/fluss-lake-paimon/pom.xml:107-127`. **Do not remove.** Paimon's `FileStoreTable`/`SerializableConfiguration` write path takes an `org.apache.hadoop.conf.Configuration` as a hard API parameter (not reflective like Iceberg). Paimon S3/OSS access also goes through `paimon-s3`/Hadoop `FileIO`. Leave Hadoop `provided` here and document it as the one tiering lake that still requires Hadoop until Paimon's native FileIO is adopted. This is the only "still needs Hadoop" exception on the tiering path.

---

## (2) Move `fluss-fs-*` cloud + Scala/Spark behind opt-in Maven profiles

### 2a. `fluss-filesystems/pom.xml` — split default vs opt-in modules

File: `/Users/bengamble/fluss/fluss-filesystems/pom.xml`. Replace the flat `<modules>` block (lines 31–40) with a lean default set, and move the heavy cloud FS modules into profiles:

```xml
<modules>
    <module>fluss-fs-hadoop</module>
    <module>fluss-fs-hadoop-shaded</module>
    <module>fluss-fs-s3</module>
    <module>fluss-fs-hdfs</module>
    <module>fluss-fs-gs</module>
</modules>

<profiles>
    <profile>
        <id>fs-extra-clouds</id>
        <!-- opt-in: Aliyun OSS, Huawei OBS, Azure ABFS, Tencent COS (all Hadoop-shaded) -->
        <modules>
            <module>fluss-fs-oss</module>
            <module>fluss-fs-obs</module>
            <module>fluss-fs-azure</module>
            <module>fluss-fs-cos</module>
        </modules>
    </profile>
</profiles>
```

Build with `-Pfs-extra-clouds` to get OSS/OBS/Azure/COS. (`fluss-fs-gs` is kept in default because GCS has no non-Hadoop alternative yet and is widely used; if you want it gated too, move its `<module>` into the profile.)

### 2b. Root `pom.xml` — gate Scala/Spark out of the default reactor

File: `/Users/bengamble/fluss/pom.xml`. Remove `<module>fluss-spark</module>` from the default `<modules>` list (line 76) and add a profile alongside the existing `scala-2.13` profile (after line 499):

```xml
<profile>
    <id>spark</id>
    <modules>
        <module>fluss-spark</module>
    </modules>
</profile>
```

Default builds (`./mvnw clean install`) then skip all Scala compilation, scalatest, scalastyle, scalafmt and Spark 3.4/3.5. CI / release builds opt in with `-Pspark` (and `-Pspark,scala-2.13` for the 2.13 cross-build). The root Scala/Spark version properties (`pom.xml:112-117`) and the `dependencyManagement` for Spark stay so the profiled modules still resolve.

### 2c. `fluss-dist/pom.xml` — keep cloud FS bundling optional

File: `/Users/bengamble/fluss/fluss-dist/pom.xml`. The dist already only hard-depends on `fluss-fs-hdfs`, `fluss-fs-oss`, `fluss-fs-s3` (lines 45–59). Move the `fluss-fs-oss` dependency under a `<profile>fs-extra-clouds</profile>` in this POM mirroring 2a so a default dist doesn't drag OSS (and transitively `hadoop-aliyun`) into the tarball. S3 + HDFS remain default.

---

## (3) maven-enforcer banned-dependencies — keep Hadoop/Flink off the tiering path

Add a **new enforcer execution local to the tiering-path POMs** (not the root, which still legitimately manages Hadoop for the FS modules). Put this in `/Users/bengamble/fluss/fluss-lake/fluss-lake-iceberg/pom.xml` inside `<build><plugins>` (alongside the existing compiler/surefire/shade plugins, ~line 254). The `org.apache.flink:*` ban is scoped to `compile`/`runtime` so the existing **test-scoped** Flink deps (lines 209–250) still pass.

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-enforcer-plugin</artifactId>
    <executions>
        <execution>
            <id>ban-hadoop-flink-on-tiering</id>
            <goals>
                <goal>enforce</goal>
            </goals>
            <configuration>
                <rules>
                    <bannedDependencies>
                        <excludes>
                            <exclude>org.apache.hadoop:*</exclude>
                            <exclude>org.apache.flink:*</exclude>
                        </excludes>
                        <includes>
                            <!-- Flink is allowed only in test scope (ITCases) -->
                            <include>org.apache.flink:*:*:*:test</include>
                            <!-- Hadoop allowed only as a test fixture if an ITCase needs it -->
                            <include>org.apache.hadoop:*:*:*:test</include>
                        </includes>
                        <searchTransitive>true</searchTransitive>
                        <message>
                            fluss-lake-iceberg must stay Hadoop-free and Flink-free on the
                            compile/runtime classpath (roadmap 0020 Phase 5). Use Iceberg
                            S3FileIO + AWS SDK v2; keep Flink in test scope only.
                        </message>
                    </bannedDependencies>
                </rules>
            </configuration>
        </execution>
    </executions>
</plugin>
```

Add the **same execution** (with `org.apache.flink:*` fully banned, no test include, since tiering-core must be engine-agnostic) to the Phase-5 `fluss-lake-tiering-core` POM once extracted (roadmap line 121). For `fluss-flink-tiering/pom.xml` no ban applies — that module is the Flink adapter by design and is being retired in favour of the standalone service.

Do **not** add the Hadoop ban to `fluss-lake-paimon/pom.xml` (see 1d). Instead add a narrower marker there for visibility:

```xml
<!-- fluss-lake-paimon intentionally retains Hadoop (provided) -->
<!-- Paimon FileStoreTable write API requires org.apache.hadoop.conf.Configuration. -->
```

---

## (4) Caffeine 2.9.3 → 3.x and other Java-8-drop-unlocked bumps

### 4a. Caffeine

File: `/Users/bengamble/fluss/fluss-server/pom.xml:50`. Change:

```xml
<caffeine.version>2.9.3</caffeine.version>
```
to:
```xml
<caffeine.version>3.1.8</caffeine.version>
```

API verified safe: the only call sites are `Caffeine.newBuilder()`, `.maximumSize(...)`, `.expireAfterAccess(Duration...)` in `fluss-server/src/main/java` (e.g. lines 264-266, 335, plus the partial-merger/bitset caches). These signatures are byte-for-byte identical across 2.x and 3.x. The 3.x breaking change is solely the JDK 11 runtime baseline — which Phase 2 (drop Java 8) already satisfies. No source edits required.

### 4b. Other bumps unlocked by the Java 11 baseline (root `pom.xml` properties, lines 99–132)

| Property (line) | From | To | Why unlocked / note |
|---|---|---|---|
| `mockito.version` (125) | 3.4.6 | 5.14.x | Mockito 5 requires Java 11; inline mock-maker default, drops the `mockito-inline` workaround. |
| `assertj.version` (126) | 3.27.7 | 3.27.x | already current; pin only. |
| AWS SDK (new, fs-s3) | v1 1.12.319 (EOL Dec 2025) | v2 2.28.x via BOM (1a) | covered in (5) below. |
| `arrow.version` (106) | 15.0.0 | 18.x | Arrow ≥16 dropped Java 8; gated on `fluss-shaded` republish — note as a follow-up, not a same-PR bump since it is a shaded artifact (`${arrow.version}-${fluss.shaded.version}`). |
| `frocksdb.version` (120) | 6.20.3-ververica-2.0 | leave | vendor-pinned; not Java-8-related. |
| `junit5.version` (124) | 5.9.1 | 5.11.x | optional; not Java-8-gated but cheap to align with Mockito 5. |

Caffeine 3.x and Mockito 5 are the two genuinely **Java-8-blocked** bumps; do those in the Phase-5 PR. Arrow ≥16 is blocked on a `fluss-shaded` release and should be a tracked follow-up, not bundled here.

---

## (5) AWS SDK v1 → v2 migration in `fluss-fs-s3` (the real v1 footprint)

This is the largest piece and is called out separately because it is **not** just a POM edit. File: `/Users/bengamble/fluss/fluss-filesystems/fluss-fs-s3/pom.xml`.

Java footprint to migrate (`fluss-fs-s3/src/main/java`): **~110 `com.amazonaws.*` imports across 9 classes**, dominated by `com.amazonaws.services.s3.*` (93 imports) plus `securitytoken` (6), `auth` (5). Concretely: `S3FileSystem.java`, `DynamicTemporaryAWSCredentialsProvider.java`, `S3DelegationTokenProvider.java`, `S3TokenLogUtils.java`, `XmlResponsesSaxParser.java` (a vendored copy of the v1 SAX parser — **deleted** under v2, which uses a different unmarshaller).

POM edits:
- **REMOVE** lines 189–211: the five `com.amazonaws:aws-java-sdk-{core,s3,kms,dynamodb,sts}` deps.
- **REMOVE** lines 214–235: `org.apache.hadoop:hadoop-aws` (its only purpose is the S3A connector backed by SDK v1).
- **REMOVE** the `hadoop-common` compile dep at lines 49–179 *if* the custom `S3FileSystem` is rewritten to not extend Hadoop's `FileSystem`; otherwise keep a minimal `hadoop-common` `provided` for the `org.apache.hadoop.fs.FileSystem` SPI only. Recommended: keep `hadoop-common` as the FS SPI base (Fluss's `FileSystem` plugin contract wraps it) and drop only `hadoop-aws` + AWS v1.
- **ADD** (versions from the BOM in 1a):

```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>s3</artifactId>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>sts</artifactId>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>kms</artifactId>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>apache-client</artifactId>
</dependency>
```

- Update the shade-plugin relocation/exclusion (lines ~333–377) that currently targets `com.amazonaws:aws-java-sdk-s3` and the vendored `XmlResponsesSaxParser` — both go away; relocate `software.amazon.awssdk` instead.

Scope note: this v1→v2 rewrite is a code task gated behind the same PR as the iceberg-aws switch (both need SDK v2 on the classpath; the BOM in 1a serves both). It is the highest-effort item in Phase 5 and should be split into its own commit.

---

## Verification (per roadmap exit criteria, lines 134–136)

```bash
# 1. tiering+iceberg path is Hadoop/Flink-free on compile/runtime:
./mvnw -pl fluss-lake/fluss-lake-iceberg dependency:tree -Dscope=compile | grep -iE 'org.apache.hadoop|org.apache.flink'   # expect: no output
# 2. enforcer is clean:
./mvnw -pl fluss-lake/fluss-lake-iceberg enforcer:enforce
# 3. default build no longer compiles Scala/Spark:
./mvnw clean install -DskipTests            # fluss-spark absent from reactor
./mvnw clean install -DskipTests -Pspark    # fluss-spark present
# 4. existing Kafka→Arrow→Iceberg/Paimon tiering ITCases still green.
```

## Net effect

- **Removed from default Iceberg tiering path:** `hadoop-common`, `hadoop-hdfs-client` (~300 MB shaded per roadmap line 197), all transitive Hadoop. Replaced by `iceberg-aws` `S3FileIO` + AWS SDK v2.
- **AWS SDK v1 (EOL):** removed from `fluss-fs-s3`; v2 via BOM.
- **Off the default reactor (opt-in profiles):** `fluss-spark` (`-Pspark`), `fluss-fs-{oss,obs,azure,cos}` (`-Pfs-extra-clouds`), and the entire Scala toolchain.
- **Bumped:** Caffeine 2.9.3→3.1.8, Mockito 3.4.6→5.x (both Java-8-unlocked).
- **Genuinely keeps Hadoop:** `fluss-lake-paimon` (Paimon write API takes `org.apache.hadoop.conf.Configuration` directly); `fluss-fs-hdfs` / `fluss-fs-hadoop-shaded` (HDFS is Hadoop by definition); `fluss-fs-gs` (no non-Hadoop GCS connector adopted yet).
