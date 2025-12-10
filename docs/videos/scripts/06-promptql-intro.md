# Video 2.1: Introduction to PromptQL

## Video Details
- **Duration**: 12 minutes
- **Level**: Intermediate
- **Prerequisites**: Basic TDB+ knowledge (Series 1)

---

## SCRIPT

### [0:00 - 0:45] INTRO

**[SCREEN: Title card "PromptQL: AI-Powered Queries"]**

**NARRATOR:**
"What if you could query your database just by describing what you want? No SQL syntax to memorize, no complex joins to figure out. Just ask in plain English and get results."

**[TRANSITION: Split screen showing SQL query vs. PromptQL query]**

"That's exactly what PromptQL delivers. In this video, you'll learn how PromptQL understands your questions, reasons through complex queries, and even maintains conversation context."

---

### [0:45 - 2:30] WHAT IS PROMPTQL

**[SCREEN: PromptQL architecture diagram]**

**NARRATOR:**
"PromptQL is TDB+'s AI-powered query language. But it's not just a SQL translator. It's a complete reasoning engine."

**[SCREEN: Diagram showing components: Parser → Planner → Reasoner → Executor]**

"When you submit a query, PromptQL goes through several stages:"

**[HIGHLIGHT: Each component as mentioned]**

"First, semantic parsing understands your intent and extracts entities. Then the query planner builds an execution strategy. The multi-step reasoner handles complex queries that require chain-of-thought logic. Finally, the executor runs the optimized query."

"What makes this special is the LLM integration. PromptQL can connect to GPT-4, Claude, or even local models to understand ambiguous queries, infer missing context, and explain results."

---

### [2:30 - 5:00] BASIC QUERIES DEMO

**[SCREEN: Split - code editor and terminal output]**

**NARRATOR:**
"Let's start with the basics. First, I'll set up PromptQL."

**[TYPE:]**
```python
from tdbai import PromptQLEngine, LLMConfig

engine = PromptQLEngine(
    db_client=client,
    llm_config=LLMConfig(
        provider="openai",
        model="gpt-4",
        api_key="your-key"
    )
)
```

**NARRATOR:**
"Now let's try some queries. I'll start simple."

**[TYPE:]**
```python
# Simple retrieval
result = await engine.query("Show all users")
print(result)
```

**[SCREEN: Shows result table]**

**NARRATOR:**
"That was easy. Let's add some filtering."

**[TYPE:]**
```python
# Filtering
result = await engine.query("Show users from New York who joined this year")
```

**NARRATOR:**
"Notice I didn't specify a field name for location or the exact date format. PromptQL figured out I meant the 'city' field and 'created_at' for the year."

**[TYPE:]**
```python
# Aggregation
result = await engine.query("What's the average order value?")
# Result: $127.50
```

**[TYPE:]**
```python
# Counting
result = await engine.query("How many premium customers do we have?")
# Result: 4,521 premium customers
```

---

### [5:00 - 7:00] ADVANCED: MULTI-STEP REASONING

**[SCREEN: Reasoning chain visualization]**

**NARRATOR:**
"Here's where PromptQL really shines. Watch what happens with a complex query."

**[TYPE:]**
```python
result = await engine.query(
    "Find customers who spent more than the average last month, "
    "and show how their spending compares to the previous year"
)
```

**NARRATOR:**
"This query requires multiple steps. PromptQL needs to:"

**[SCREEN: Animated step-by-step breakdown]**

"One - Calculate the average spending for last month.
Two - Find customers above that average.
Three - Get their spending from the previous year.
Four - Compare and format the results."

**[SCREEN: Show result with comparison data]**

**NARRATOR:**
"All of this happens automatically. Let me show you the reasoning chain."

**[TYPE:]**
```python
# See the reasoning steps
steps = await engine.explain(
    "Find customers who spent more than average..."
)
print(steps)
```

**[SCREEN: Display reasoning steps]**
```
Step 1: Decomposing query into sub-queries
Step 2: Calculate average (SELECT AVG(total) FROM orders WHERE month = 'last')
Step 3: Filter customers above threshold
Step 4: Retrieve historical data for comparison
Step 5: Format comparison results
```

---

### [7:00 - 9:00] CONVERSATION CONTEXT

**[SCREEN: Chat-like interface demonstration]**

**NARRATOR:**
"One of PromptQL's most powerful features is conversation context. Let me show you."

**[TYPE:]**
```python
# Initial query
await engine.query("Show our top 100 customers by revenue")
```

**[SCREEN: Table of 100 customers]**

**NARRATOR:**
"Now I want to filter these results. Watch this."

**[TYPE:]**
```python
# Follow-up - understands "those" refers to previous result
await engine.query("Filter those to only enterprise accounts")
```

**[SCREEN: Filtered table]**

**NARRATOR:**
"And I can keep refining..."

**[TYPE:]**
```python
# Another follow-up
await engine.query("Now sort by last purchase date")

# And another
await engine.query("Export the top 10 to CSV")
```

**NARRATOR:**
"Each query builds on the previous context. This is how humans naturally explore data - through conversation."

---

### [9:00 - 10:30] TYPO CORRECTION & SYNONYMS

**[SCREEN: Error handling demonstration]**

**NARRATOR:**
"PromptQL is also forgiving. Watch what happens with typos."

**[TYPE:]**
```python
# Typo in query
result = await engine.query("Show cutsomers from Califronia")
# PromptQL corrects to: "Show customers from California"
```

**NARRATOR:**
"It automatically corrected 'cutsomers' to 'customers' and 'Califronia' to 'California'. It also understands synonyms."

**[TYPE:]**
```python
# Using synonyms
await engine.query("Find clients who bought stuff last week")
# Interprets as: SELECT * FROM customers WHERE orders.date > ...
```

**NARRATOR:**
"'Clients' mapped to our 'customers' collection, and 'bought stuff' to orders."

---

### [10:30 - 11:30] CONFIGURATION OPTIONS

**[SCREEN: Configuration code]**

**NARRATOR:**
"You can customize PromptQL for your needs."

**[TYPE:]**
```python
# Use different LLM providers
# OpenAI
config = LLMConfig(provider="openai", model="gpt-4")

# Anthropic Claude
config = LLMConfig(provider="anthropic", model="claude-3-opus")

# Local model (Ollama)
config = LLMConfig(
    provider="local",
    api_base="http://localhost:11434",
    model="llama2"
)
```

**NARRATOR:**
"For privacy-sensitive applications, you can run everything locally with no data leaving your servers."

**[TYPE:]**
```python
# Performance tuning
engine = PromptQLEngine(
    db_client=client,
    config=PromptQLConfig(
        cache_enabled=True,       # Cache similar queries
        max_reasoning_steps=10,   # Limit complexity
        timeout_ms=30000          # Query timeout
    )
)
```

---

### [11:30 - 12:00] SUMMARY & NEXT STEPS

**[SCREEN: Summary bullet points]**

**NARRATOR:**
"Let's recap what you've learned:
- PromptQL understands natural language queries
- Multi-step reasoning handles complex analytics
- Conversation context enables iterative exploration
- Typo correction and synonyms make it forgiving
- Multiple LLM providers including local options"

**[SCREEN: Next video thumbnail]**

"In the next video, we'll dive deeper into SQL queries for when you need precise control. See you there!"

**[SCREEN: End card with links]**

---

## DEMO DATABASE SETUP

```sql
-- Create demo data before recording
INSERT INTO customers (name, city, type, created_at) VALUES
  ('Acme Corp', 'New York', 'enterprise', '2024-01-15'),
  ('StartupXYZ', 'San Francisco', 'startup', '2024-02-20'),
  ...;

INSERT INTO orders (customer_id, total, created_at) VALUES
  (1, 5000, '2024-01-20'),
  (1, 7500, '2024-02-15'),
  ...;
```

## B-ROLL SUGGESTIONS

1. AI/neural network visualizations for LLM segments
2. Chat interface animations for context demo
3. Code typing close-ups
4. Split-screen before/after comparisons

## GRAPHICS NEEDED

1. PromptQL logo
2. Architecture diagram (Parser → Planner → Reasoner → Executor)
3. Reasoning chain visualization
4. Conversation context animation
5. Provider logos (OpenAI, Anthropic, Ollama)
