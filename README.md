# Quickstore – fast and thread-safe key-value store with low contention and builtin cache
[![Go Report Card](https://goreportcard.com/badge/github.com/erathorus/quickstore)](https://goreportcard.com/report/github.com/erathorus/quickstore)

Quickstore is an opinionated key-value store built on top of DynamoDB.

## Features:
 - Builtin in-memory cache.
 - Async write: data are written using [write-back policy](https://en.wikipedia.org/wiki/Cache_(computing)#Writing_policies).
 - Low thread contention since data entries are partitioned into multiple nodes, allowing efficient parallelism.

## Restriction:
 - Primary key contains only partition key to avoid hot partition problem.
