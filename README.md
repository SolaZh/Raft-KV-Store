# Raft Key-Value Store (ECE419 Project)

## Overview

This project is an implementation of the **Raft consensus algorithm** in Go, built as part of the ECE419 Distributed Systems course.

The system provides a replicated key-value store that remains consistent across multiple nodes even in the presence of failures such as leader crashes or network delays.

It implements the core components of Raft:
- Leader election
- Log replication
- Safety guarantees for distributed consensus
- Persistent state recovery
