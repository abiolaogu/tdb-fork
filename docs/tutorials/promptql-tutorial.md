# PromptQL Tutorial

## Complete Guide to AI-Powered Queries

### Quick Start

```python
from tdbai import PromptQLEngine, LLMConfig

# Initialize
engine = PromptQLEngine(
    db_client=client,
    llm_config=LLMConfig(provider="openai", model="gpt-4", api_key="your-key")
)

# Query in natural language
result = await engine.query("Show all active users from California")
```

### Query Examples

| Intent | Query | Example |
|--------|-------|---------|
| Retrieve | "Show all X" | `Show all premium customers` |
| Count | "How many X" | `How many orders this week?` |
| Aggregate | "Average/Sum/Total" | `What's the average order value?` |
| Compare | "Compare X vs Y" | `Compare Q1 vs Q2 sales` |
| Trend | "X over time" | `Show revenue trend last 12 months` |
| Top N | "Top/Best X" | `Top 10 products by revenue` |

### Conversation Context

```python
await engine.query("Show top 100 customers")       # Initial query
await engine.query("Filter to enterprise only")    # Refines previous
await engine.query("Sort by revenue descending")   # Further refines
await engine.query("Export as CSV")                # Acts on result
```

### Multi-Step Reasoning

```python
# Complex query requiring multiple steps
result = await engine.query(
    "Find customers who spent more than average last month "
    "and compare their behavior to the previous year"
)
# PromptQL automatically:
# 1. Calculates average spending
# 2. Filters customers above average
# 3. Retrieves historical data
# 4. Computes comparison
```

### Configuration Options

```python
# OpenAI
LLMConfig(provider="openai", model="gpt-4")

# Anthropic Claude
LLMConfig(provider="anthropic", model="claude-3-opus")

# Local (Ollama)
LLMConfig(provider="local", api_base="http://localhost:11434", model="llama2")
```

### Best Practices

1. **Be specific** - "users from New York" vs "users"
2. **Use context** - Follow-up queries build on previous
3. **Request explanations** - `await engine.explain("Why...")`
4. **Check suggestions** - `await engine.suggest("I want to...")`
