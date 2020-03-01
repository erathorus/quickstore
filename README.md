# Quickstore – fast and thread-safe key-value store with low contention and builtin cache
[![Go Report Card](https://goreportcard.com/badge/github.com/erathorus/quickstore)](https://goreportcard.com/report/github.com/erathorus/quickstore)

Quickstore acts as a cache layer on top of DynamoDB. All mutations are performed in Quickstore before being written
 out to DynamoDB, thus allowing extremely fast read/write and low network latency.

## Features:
 - In-memory database performance.
 - Low thread contention since data entries are partitioned into multiple nodes, allowing efficient parallelism.
 - Reduce number of call to DynamoDB using builtin-cache, cut down costs and reduce network latency to minimal.
 - Mutations are written out in batch using transaction, ensuring data consistency.
 - Gracefully handling crash by logging unwritten mutations.

## Restriction:
 - Primary key contains only partition key to avoid hot partition.
