# LumaDB Multi-Protocol Deployment Guide

This guide covers the deployment of the LumaDB multi-protocol server in production environments using Docker and Kubernetes.

## 🐳 Docker Deployment

For local development or simple deployments, use Docker Compose.

### Prerequisites
- Docker Engine
- Docker Compose

### Quick Start
```bash
cd crates/lumadb-multicompat
docker-compose up -d
```
This starts:
- LumaDB Server (:8000)
- Prometheus (:9090)
- Grafana (:3000) - Default login: admin/admin

### Health Checks
```bash
curl http://localhost:8000/health
```

## ☸️ Kubernetes Deployment

For production deployments, use the provided Kubernetes manifests.

### Prerequisites
- Kubernetes Cluster (v1.24+)
- kubectl configured
- Persistent Storage support

### Deployment Steps

1. **Create Namespace**
   ```bash
   kubectl create namespace lumadb
   ```

2. **Configure Secrets**
   Base64 encode your secrets and update `kubernetes/deployment.yaml`.
   ```bash
   echo -n "my-secret-key" | base64
   ```

3. **Deploy Resources**
   ```bash
   kubectl apply -f kubernetes/deployment.yaml
   ```

4. **Verify Deployment**
   ```bash
   kubectl get pods -n lumadb
   kubectl get svc -n lumadb
   ```

### Horizontal Pod Autoscaling (HPA)
The included HPA configuration automatically scales pods based on CPU utilization (target 70%).
```yaml
minReplicas: 3
maxReplicas: 10
```

## 📊 Monitoring & Observability

### Prometheus
The server exposes metrics at `/metrics` conforming to Prometheus standards.

**Key Metrics:**
- `http_requests_total`: Total request count by protocol/status
- `http_request_duration_seconds`: Request latency histogram
- `lumadb_cache_hits_total`: Query cache hits
- `lumadb_cache_misses_total`: Query cache misses

### Grafana
A pre-configured dashboard is available at `grafana/dashboards/lumadb.json`.
Import this JSON into your Grafana instance to visualize:
- RPS by Protocol
- Latency (p95, p99)
- Cache Efficiency
- Error Rates

## 🔒 Security Hardening

1. **Non-Root User**: The Docker image runs as a non-root user (`lumadb`, UID 1000).
2. **Read-Only Root Filesystem**: Configure Kubernetes security context.
   ```yaml
   securityContext:
     readOnlyRootFilesystem: true
   ```
3. **Secrets Management**: Use Kubernetes Secrets or Vault for credentials. Do not hardcode APIs keys.
4. **Network Policies**: Restrict ingress traffic to port 8000 and metrics port.

## 📈 Scaling Guide

**Vertical Scaling:**
- Increase CPU limits if latency spikes under load.
- Increase Memory requests if cache eviction rate is high.

**Horizontal Scaling:**
- LumaDB requires a shared storage backend (e.g., standard LumaDB distributed layer) for consistent data across replicas.
- If using `memory` storage (dev/test), replicas are independent and **will not share data**.

## 💾 Backup & Recovery

1. **Volume Snapshots**: Configure your Cloud Provider to snapshot the PersistentVolumeClaim (`lumadb-pvc`).
2. **Export Data**: Use standard protocol tools to dump data.
   - DynamoDB: `Scan` operation
   - SQL: `SELECT * FROM ...`
