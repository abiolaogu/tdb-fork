# Universal Multi-Query Language Support

LumaDB supports 11+ time-series database query languages natively, allowing you to use your existing queries without modification.

## Supported Dialects

| Dialect | Target Database | Example |
|---------|-----------------|---------|
| **InfluxQL** | InfluxDB 1.x | `SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(5m)` |
| **Flux** | InfluxDB 2.x/3.x | `from(bucket:"test") \|> range(start:-1h) \|> filter(fn:(r) => r._measurement == "cpu")` |
| **PromQL** | Prometheus | `rate(http_requests_total[5m])` |
| **MetricsQL** | VictoriaMetrics | `topk_avg(10, rate(requests[5m]))` |
| **TimescaleDB** | TimescaleDB | `SELECT time_bucket('1 hour', time), avg(value) FROM metrics` |
| **QuestDB** | QuestDB | `SELECT * FROM trades SAMPLE BY 1h` |
| **ClickHouse** | ClickHouse | `SELECT toStartOfHour(time), avg(value) FROM metrics GROUP BY 1` |
| **Druid SQL** | Apache Druid | `SELECT FLOOR(__time TO HOUR), AVG(value) FROM datasource GROUP BY 1` |
| **Druid Native** | Apache Druid | `{"queryType":"timeseries","dataSource":"metrics"}` |
| **OpenTSDB** | OpenTSDB | `{"queries":[{"aggregator":"sum","metric":"sys.cpu"}]}` |
| **Graphite** | Graphite | `summarize(server.*.cpu.usage, "1h", "avg")` |

## Auto-Detection

LumaDB automatically detects the query dialect:

```bash
# Auto-detect endpoint
curl -X POST http://localhost:8080/dialect/auto \
  -d '{"query": "rate(http_requests_total[5m])"}'

# Response
{
  "detected_dialect": "promql",
  "confidence": 0.95,
  "data": { ... }
}
```

## Native Protocol Endpoints

| Endpoint | Dialect | Method |
|----------|---------|--------|
| `/api/v1/query` | PromQL | GET/POST |
| `/query?db=...&q=...` | InfluxQL | GET |
| `/api/v2/query` | Flux | POST |
| `/druid/v2` | Druid | POST |
| `/api/query` | OpenTSDB | POST |
| `/render` | Graphite | GET/POST |
| `/exec` | QuestDB | GET/POST |

## Cross-Dialect Translation

Translate queries between dialects:

```bash
curl -X POST http://localhost:8080/translate \
  -d '{
    "query": "rate(http_requests_total[5m])",
    "from": "promql",
    "to": "influxql"
  }'
```

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Query Input    │ --> │ Dialect Detector │ --> │ Dialect Parser  │
│ (Any Dialect)   │     │ (Pattern Match)  │     │ (Rust Core)     │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
                                                          v
                                                 ┌─────────────────┐
                                                 │  Unified IR     │
                                                 │  (QueryPlan)    │
                                                 └────────┬────────┘
                                                          │
                              ┌────────────────┬──────────┴──────────┬────────────────┐
                              v                v                     v                v
                      ┌─────────────┐  ┌─────────────┐       ┌─────────────┐  ┌─────────────┐
                      │ LumaDB      │  │ InfluxQL    │       │ PromQL      │  │ SQL         │
                      │ Executor    │  │ Translator  │       │ Translator  │  │ Translator  │
                      └─────────────┘  └─────────────┘       └─────────────┘  └─────────────┘
```

## Implementation

- **Rust Core**: 11 dialect parsers with full test coverage
- **Go Router**: HTTP handlers for all protocol endpoints
- **Python AI**: ML-based dialect detection (future)

---

**See Also:**
- [TDengine Compatibility](tdengine-compatibility.md)
- [API Reference](api-reference/)
