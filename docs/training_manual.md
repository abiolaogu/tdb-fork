# LumaDB Training Manual

## Course Overview
This training program is designed to take you from a LumaDB novice to an expert administrator and developer.

**Duration:** 2 Days (16 Hours)
**Prerequisites:** Basic knowledge of databases (SQL) and Linux terminal.

---

## Module 1: Introduction to LumaDB (2 Hours)
**Goal:** Understand the architecture and unique value proposition.

- **1.1 The Evolution of Databases**: From RDBMS to NoSQL to NewSQL / AI-Native.
- **1.2 LumaDB Architecture**:
  - Hybrid Memory Tiering (RAM/SSD).
  - Shard-per-Core Design.
  - The Distributed Cluster (Raft).
- **1.3 Lab 1**: Installing LumaDB using Docker.

## Module 2: Developing with LumaDB (4 Hours)
**Goal:** Learn how to build applications using LumaDB.

- **2.1 The Unified Query Interface**: LQL, NQL, JQL.
- **2.2 Data Modeling**: Schemaless vs. Schema-enforced collections.
- **2.3 Using the SDKs**: TypeScript and Python examples.
- **2.4 AI Integration**: Storing vectors and running semantic seach.
- **2.5 Lab 2**: Building a simple "Product Catalog" API with semantic search.

## Module 3: Storage & Performance Tuning (4 Hours)
**Goal:** Master the storage engine configuration.

- **3.1 Understanding LSM-Trees and Write Amplification**.
- **3.2 Configuring Tiering Policies**:
  - Defining Hot/Warm/Cold thresholds.
  - Erasure Coding vs. Replication.
- **3.3 Monitoring**: Understanding the Metrics API and Dashboards.
- **3.4 Lab 3**: Simulating load and observing data migration to SSD.

## Module 4: Cluster Administration (4 Hours)
**Goal:** Managing a production LumaDB cluster.

- **4.1 Cluster Topology**: Joining nodes and rebalancing.
- **4.2 Backup & Restore**: Snapshotting and Point-in-Time Recovery.
- **4.3 Upgrades**: Rolling upgrades without downtime.
- **4.4 Security**:
  - **Authentication**: Generating JWT tokens via `/api/auth/login`.
  - **Authorization**: Securing API endpoints with Middleware.
  - **RBAC**: Managing user roles.
- **4.5 Lab 4**: Setting up a 3-node HA cluster and simulating a node failure.

## Module 5: Final Assessment (2 Hours)
- **Exam**: 30 Multiple choice questions.
- **Capstone Project**: Deploy a resilient cluster with a specific storage policy.
