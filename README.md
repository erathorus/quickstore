# Quickstore – fast and thread-safe key-value store with low contention and builtin cache
Quickstore is an opinionated key-value store built on top of DynamoDB.

## Features:
 - Instant read: data entries are cached in-memory.
 - Instant write: data entries are written using the [write-back policy](https://en.wikipedia.org/wiki/Cache_(computing)#Writing_policies).
 - Low contention: data entries are partitioned into multiple nodes, allowing efficient parallelism.

## Restriction:
 - Primary key contains only partition key to avoid hot partition problem.
