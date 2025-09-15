---
title: Upgrade Notes
sidebar_position: 3
---

# Upgrade Notes from v0.7 to v0.8

These upgrade notes discuss important aspects, such as configuration, behavior, or dependencies, that changed between Fluss 0.7 and Fluss 0.8. Please read these notes carefully if you are planning to upgrade your Fluss version to 0.8.

## Deprecation / End of Support

### Java 8 is Deprecated
Beginning with Fluss v0.8, we now only provide binary distributions built with Java 11.
**Java 8 is deprecated** as of this release and will be fully removed in future versions.

🔧 **For users still on Java 8**:
You can continue building Fluss from source using Java 8 by running:
```bash
mvn install -DskipTests -Pjava8
```
However, we **strongly recommend upgrading to Java 11 or higher** to ensure compatibility, performance, and long-term support.

🔁 **If you’re using Fluss with Apache Flink**:
Please also upgrade your Flink deployment to **Java 11 or above**. All Flink versions currently supported by Fluss are fully compatible with Java 11.

## Metrics Updates

We have updated the report level for some metrics and also removed some metrics, this greatly reduces the metrics amount and improves the performance.

The following metrics are removed: TODO

The following metrics are changed: TODO