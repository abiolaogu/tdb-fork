# LumaDB Video Training Scripts

## Series 1: Getting Started

### Video 1.1: What is LumaDB?
**Duration:** 3 mins
**Visuals:** Animated architecture diagram, logo, performance charts.

**Script:**
> **[Intro Music]**
> **Host:** "Hello and welcome! Today we're introducing LumaDB, the database that's changing the way we think about storage and AI."
> **[Cut to Architecture Diagram]**
> **Host:** "Traditional databases force you to choose: speed OR scale. Analytics OR Transactions. LumaDB gives you both."
> "At its core is a Rust-based, high-performance engine that utilizes a generic 'Hybrid Memory Architecture'. This means your hot data lives in RAM for speed, while warm data automatically moves to SSDs to save cost."
> **[Cut to AI Feature Demo]**
> "But that's not all. LumaDB is AI-Native. You can store vector embeddings right alongside your JSON documents and query them with natural language."
> **[Outro]** "In the next video, we'll install LumaDB in less than 5 minutes. Stay tuned."

### Video 1.2: Installation & Setup
**Duration:** 5 mins
**Visuals:** Screencast of terminal/IDE.

**Script:**
> **Host:** "Let's get LumaDB running. The easiest way is Docker."
> **[Show Terminal]**
> "Type `docker run -p 8080:8080 lumadb/server`. And... BOOM. We are live."
> "Let's verify it with curl. `curl localhost:8080/version`. There it is: Version 2.0.0."
> "Now, let's look at the configuration file `luma.toml`..."

---

## Series 2: Advanced Features

### Video 2.1: Storage Tiering Implementation
**Duration:** 8 mins
**Visuals:** Diagram of data movement, Dashboard showing I/O stats.

**Script:**
> **Host:** "Storing petabytes in RAM is expensive. LumaDB solves this with Tiering."
> "Open `luma.toml`. Look at the `[tiering]` section."
> "We can set `age_threshold = 3600`. This tells the engine: if data hasn't been touched in an hour, move it to the 'Warm' SSD tier."
> "For the Cold tier, we use Erasure Coding. It's like RAID-6 but across the network. It saves you 50% storage overhead compared to standard 3-copy replication."
> **[Show Diagram of EC]**
> "Data is split into shards + parity chunks. If a drive fails, we rebuild mathmatically."

### Video 2.2: AI & Vector Search
**Duration:** 6 mins
**Visuals:** Code editor, Python script.

**Script:**
> **Host:** "Forget managing a separate Vector DB like Pinecone or Milvus. LumaDB does it natively."
> "Let's insert a product with an embedding."
> **[Typing Code]**
> "See here? `db.insert('products', { name: 'Shoe', embedding: [...] })`."
> "Now let's search: `db.ai.search('products', vector, top_k=5)`."
> "It uses the internal FAISS index to find neighbors instantly."
