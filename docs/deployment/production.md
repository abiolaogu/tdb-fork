# TDB+ Production Deployment Guide

## Complete Guide to Deploying TDB+ in Production

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Hardware Requirements](#hardware-requirements)
3. [Installation Methods](#installation-methods)
4. [Configuration](#configuration)
5. [Cluster Setup](#cluster-setup)
6. [Security](#security)
7. [Monitoring](#monitoring)
8. [Backup & Recovery](#backup--recovery)
9. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### System Requirements

| Component | Minimum | Recommended | Production |
|-----------|---------|-------------|------------|
| **CPU** | 4 cores | 16 cores | 32+ cores |
| **RAM** | 8 GB | 64 GB | 256+ GB |
| **Storage** | 100 GB SSD | 1 TB NVMe | 10+ TB NVMe |
| **Network** | 1 Gbps | 10 Gbps | 25+ Gbps |
| **OS** | Ubuntu 20.04+ | Ubuntu 22.04 | Ubuntu 22.04 LTS |

### Software Dependencies

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install -y \
    build-essential \
    libssl-dev \
    pkg-config \
    liblz4-dev \
    libzstd-dev \
    libnuma-dev

# Enable io_uring (Linux 5.1+)
# Verify support:
cat /proc/version  # Should show 5.1 or higher
```

---

## Hardware Requirements

### Memory Sizing

```
Total RAM Required = Primary Index + Hot Data + Block Cache + OS

Primary Index:   ~64 bytes per record
Hot Data:        Your hot dataset size
Block Cache:     10-20% of total RAM
OS Reserved:     8-16 GB minimum

Example (1 billion records, 10GB hot data):
- Primary Index: 1B × 64 bytes = 64 GB
- Hot Data: 10 GB
- Block Cache: 20 GB
- OS Reserved: 16 GB
- Total: ~110 GB RAM recommended
```

### Storage Sizing

```
Storage Required = Total Data × Compression Ratio × Replication Factor + WAL

Compression Ratios (typical):
- Integer columns: 4-10x
- String columns: 2-4x
- JSON documents: 2-3x

Example (100GB raw data, 3x replication):
- Compressed: 100GB / 3 = ~33GB
- Replicated: 33GB × 3 = ~100GB
- WAL buffer: 10GB
- Total: ~110GB per node
```

### Network Requirements

| Cluster Size | Inter-node Traffic | Client Traffic |
|--------------|-------------------|----------------|
| 3 nodes | 1 Gbps | 1 Gbps |
| 10 nodes | 10 Gbps | 10 Gbps |
| 50+ nodes | 25 Gbps | 25 Gbps |

---

## Installation Methods

### Method 1: Docker (Recommended for Quick Start)

```yaml
# docker-compose.yml
version: '3.8'

services:
  tdbplus:
    image: tdbplus/tdbplus:2.0.0
    container_name: tdbplus
    ports:
      - "8080:8080"   # HTTP API
      - "9090:9090"   # gRPC API
      - "7000:7000"   # Cluster communication
    volumes:
      - tdbplus-data:/data
      - ./config.yaml:/etc/tdbplus/config.yaml
    environment:
      - TDB_CLUSTER_NAME=production
      - TDB_NODE_ID=node1
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
    deploy:
      resources:
        limits:
          memory: 64G
        reservations:
          memory: 32G

volumes:
  tdbplus-data:
    driver: local
```

```bash
# Start TDB+
docker-compose up -d

# Check status
docker-compose logs -f tdbplus
```

### Method 2: Kubernetes

```yaml
# tdbplus-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: tdbplus
  namespace: database
spec:
  serviceName: tdbplus
  replicas: 3
  selector:
    matchLabels:
      app: tdbplus
  template:
    metadata:
      labels:
        app: tdbplus
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchExpressions:
              - key: app
                operator: In
                values:
                - tdbplus
            topologyKey: kubernetes.io/hostname
      containers:
      - name: tdbplus
        image: tdbplus/tdbplus:2.0.0
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 9090
          name: grpc
        - containerPort: 7000
          name: cluster
        env:
        - name: TDB_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: TDB_CLUSTER_SEEDS
          value: "tdbplus-0.tdbplus.database.svc.cluster.local,tdbplus-1.tdbplus.database.svc.cluster.local,tdbplus-2.tdbplus.database.svc.cluster.local"
        resources:
          requests:
            memory: "32Gi"
            cpu: "8"
          limits:
            memory: "64Gi"
            cpu: "16"
        volumeMounts:
        - name: data
          mountPath: /data
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 500Gi
---
apiVersion: v1
kind: Service
metadata:
  name: tdbplus
  namespace: database
spec:
  clusterIP: None
  ports:
  - port: 8080
    name: http
  - port: 9090
    name: grpc
  - port: 7000
    name: cluster
  selector:
    app: tdbplus
---
apiVersion: v1
kind: Service
metadata:
  name: tdbplus-lb
  namespace: database
spec:
  type: LoadBalancer
  ports:
  - port: 8080
    name: http
  - port: 9090
    name: grpc
  selector:
    app: tdbplus
```

### Method 3: Native Installation

```bash
#!/bin/bash
# install-tdbplus.sh

set -e

# Download latest release
VERSION="2.0.0"
curl -LO "https://releases.tdbplus.io/v${VERSION}/tdbplus-linux-amd64.tar.gz"
tar xzf "tdbplus-linux-amd64.tar.gz"

# Install binaries
sudo mv tdbplus /usr/local/bin/
sudo mv tdbplus-cli /usr/local/bin/

# Create directories
sudo mkdir -p /var/lib/tdbplus/data
sudo mkdir -p /var/lib/tdbplus/wal
sudo mkdir -p /var/log/tdbplus
sudo mkdir -p /etc/tdbplus

# Create service user
sudo useradd -r -s /bin/false tdbplus
sudo chown -R tdbplus:tdbplus /var/lib/tdbplus
sudo chown -R tdbplus:tdbplus /var/log/tdbplus

# Install systemd service
sudo cat > /etc/systemd/system/tdbplus.service << 'EOF'
[Unit]
Description=TDB+ Database Server
After=network.target

[Service]
Type=simple
User=tdbplus
Group=tdbplus
ExecStart=/usr/local/bin/tdbplus server --config /etc/tdbplus/config.yaml
Restart=always
RestartSec=5
LimitNOFILE=65536
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable tdbplus
sudo systemctl start tdbplus
```

---

## Configuration

### Main Configuration File

```yaml
# /etc/tdbplus/config.yaml

# Server configuration
server:
  http_port: 8080
  grpc_port: 9090
  cluster_port: 7000
  max_connections: 10000

# Storage configuration
storage:
  data_dir: /var/lib/tdbplus/data
  wal_dir: /var/lib/tdbplus/wal

  # Hybrid memory settings (Aerospike-style)
  hybrid:
    enabled: true
    ram_percentage: 30          # Hot data in RAM
    ssd_path: /var/lib/tdbplus/ssd
    defrag_threshold: 50

  # Columnar engine settings
  columnar:
    enabled: true
    compression: zstd
    block_size: 65536

# Memory configuration
memory:
  max_memory_gb: 64
  block_cache_gb: 16
  memtable_size_mb: 256
  numa_aware: true

# Cluster configuration
cluster:
  name: production
  node_id: node1
  seeds:
    - node1.tdbplus.local:7000
    - node2.tdbplus.local:7000
    - node3.tdbplus.local:7000
  replication_factor: 3
  consistency_level: quorum

# Performance tuning
performance:
  io_uring_enabled: true
  io_threads: 8
  compaction_threads: 4
  flush_interval_ms: 1000
  group_commit_size: 100

# PromptQL configuration
promptql:
  enabled: true
  llm_provider: openai
  llm_model: gpt-4
  cache_enabled: true
  max_reasoning_steps: 10

# Logging
logging:
  level: info
  file: /var/log/tdbplus/tdbplus.log
  max_size_mb: 100
  max_files: 10

# Metrics
metrics:
  enabled: true
  prometheus_port: 9100
```

### Environment-Specific Overrides

```bash
# Production environment variables
export TDB_SERVER_HTTP_PORT=8080
export TDB_STORAGE_DATA_DIR=/data/tdbplus
export TDB_MEMORY_MAX_MEMORY_GB=256
export TDB_CLUSTER_REPLICATION_FACTOR=3
export TDB_PROMPTQL_LLM_API_KEY=your-production-key
```

---

## Cluster Setup

### 3-Node Cluster Example

```
┌─────────────────────────────────────────────────────────────┐
│                        Load Balancer                         │
│                    (HAProxy/AWS ALB/etc)                    │
└───────────────────────────┬─────────────────────────────────┘
                            │
         ┌──────────────────┼──────────────────┐
         │                  │                  │
         ▼                  ▼                  ▼
    ┌─────────┐        ┌─────────┐        ┌─────────┐
    │  Node 1 │◄──────►│  Node 2 │◄──────►│  Node 3 │
    │         │        │         │        │         │
    │ Primary │        │ Primary │        │ Primary │
    │ Replica │        │ Replica │        │ Replica │
    │         │        │         │        │         │
    └─────────┘        └─────────┘        └─────────┘
         │                  │                  │
         ▼                  ▼                  ▼
    ┌─────────┐        ┌─────────┐        ┌─────────┐
    │  NVMe   │        │  NVMe   │        │  NVMe   │
    │ Storage │        │ Storage │        │ Storage │
    └─────────┘        └─────────┘        └─────────┘
```

### Node Configuration

**Node 1 (config-node1.yaml):**
```yaml
cluster:
  name: production
  node_id: node1
  seeds:
    - node1.tdbplus.local:7000
    - node2.tdbplus.local:7000
    - node3.tdbplus.local:7000
```

**Node 2 (config-node2.yaml):**
```yaml
cluster:
  name: production
  node_id: node2
  seeds:
    - node1.tdbplus.local:7000
    - node2.tdbplus.local:7000
    - node3.tdbplus.local:7000
```

**Node 3 (config-node3.yaml):**
```yaml
cluster:
  name: production
  node_id: node3
  seeds:
    - node1.tdbplus.local:7000
    - node2.tdbplus.local:7000
    - node3.tdbplus.local:7000
```

### Verify Cluster

```bash
# Check cluster status
tdbplus-cli cluster status

# Expected output:
# Cluster: production
# Nodes: 3/3 healthy
# ┌──────────┬─────────────────────────┬─────────┬─────────┐
# │ Node ID  │ Address                 │ Status  │ Shards  │
# ├──────────┼─────────────────────────┼─────────┼─────────┤
# │ node1    │ node1.tdbplus.local:7000│ ONLINE  │ 33      │
# │ node2    │ node2.tdbplus.local:7000│ ONLINE  │ 33      │
# │ node3    │ node3.tdbplus.local:7000│ ONLINE  │ 34      │
# └──────────┴─────────────────────────┴─────────┴─────────┘
```

---

## Security

### TLS Configuration

```yaml
# config.yaml
security:
  tls:
    enabled: true
    cert_file: /etc/tdbplus/certs/server.crt
    key_file: /etc/tdbplus/certs/server.key
    ca_file: /etc/tdbplus/certs/ca.crt
    client_auth: require  # none, request, require

  # Authentication
  auth:
    enabled: true
    method: jwt  # basic, jwt, ldap

  # Authorization
  authorization:
    enabled: true
    rbac_enabled: true
```

### Create TLS Certificates

```bash
#!/bin/bash
# generate-certs.sh

# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
  -subj "/CN=TDBPlus CA"

# Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
  -subj "/CN=tdbplus.local"
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt

# Install certificates
sudo mkdir -p /etc/tdbplus/certs
sudo cp ca.crt server.crt server.key /etc/tdbplus/certs/
sudo chmod 600 /etc/tdbplus/certs/*.key
```

### Create Users and Roles

```bash
# Create admin user
tdbplus-cli user create admin --role admin --password <password>

# Create application user
tdbplus-cli user create app_user --role readwrite --password <password>

# Create read-only user
tdbplus-cli user create report_user --role readonly --password <password>

# Create custom role
tdbplus-cli role create analyst \
  --permissions "read:*" \
  --permissions "aggregate:*" \
  --deny "write:*"
```

---

## Monitoring

### Prometheus Metrics

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'tdbplus'
    static_configs:
      - targets:
        - 'node1.tdbplus.local:9100'
        - 'node2.tdbplus.local:9100'
        - 'node3.tdbplus.local:9100'
```

### Key Metrics to Monitor

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `tdb_read_latency_p99` | Read latency | > 10ms |
| `tdb_write_latency_p99` | Write latency | > 50ms |
| `tdb_memory_used_bytes` | Memory usage | > 90% |
| `tdb_disk_used_bytes` | Disk usage | > 80% |
| `tdb_connections_active` | Active connections | > 80% of max |
| `tdb_replication_lag_ms` | Replication lag | > 1000ms |

### Grafana Dashboard

Import the TDB+ dashboard from: `https://grafana.com/grafana/dashboards/tdbplus`

---

## Backup & Recovery

### Automated Backups

```bash
#!/bin/bash
# backup.sh - Run daily via cron

BACKUP_DIR=/backups/tdbplus
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_PATH="${BACKUP_DIR}/backup_${DATE}"

# Create backup
tdbplus-cli backup create \
  --output ${BACKUP_PATH} \
  --compression zstd \
  --parallel 4

# Upload to S3
aws s3 sync ${BACKUP_PATH} s3://backups/tdbplus/${DATE}/

# Cleanup old backups (keep 7 days)
find ${BACKUP_DIR} -type d -mtime +7 -exec rm -rf {} \;
```

### Restore from Backup

```bash
# Restore full backup
tdbplus-cli backup restore \
  --input /backups/tdbplus/backup_20240101_120000 \
  --parallel 4

# Point-in-time recovery
tdbplus-cli backup restore \
  --input /backups/tdbplus/backup_20240101_120000 \
  --point-in-time "2024-01-15T14:30:00Z"
```

---

## Troubleshooting

### Common Issues

**Issue: High Memory Usage**
```bash
# Check memory breakdown
tdbplus-cli debug memory

# Solution: Tune block cache
# config.yaml:
memory:
  block_cache_gb: 8  # Reduce if needed
```

**Issue: Slow Queries**
```bash
# Enable slow query log
tdbplus-cli config set logging.slow_query_threshold_ms 100

# Check slow queries
tail -f /var/log/tdbplus/slow_queries.log
```

**Issue: Cluster Node Down**
```bash
# Check node status
tdbplus-cli cluster node-status node2

# Force node removal (if unrecoverable)
tdbplus-cli cluster remove-node node2 --force
```

### Health Checks

```bash
# Full health check
tdbplus-cli health --verbose

# Expected output:
# ✓ Storage: healthy (85% free)
# ✓ Memory: healthy (62% used)
# ✓ Cluster: healthy (3/3 nodes)
# ✓ Replication: healthy (lag < 100ms)
# ✓ Connections: healthy (234/10000)
```

---

## Next Steps

- [High Availability Guide](./ha.md)
- [Disaster Recovery](./dr.md)
- [Performance Tuning](../performance/optimization.md)
