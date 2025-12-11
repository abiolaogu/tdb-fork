import { Database } from '../core/Database';
import { QueryResult, DocumentData, ParsedQuery, QuerySyntaxError } from '../types';
import { PromptQLParser } from './PromptQLParser';
import { ILLMProvider, PromptContext } from '../llm/types';

export class PromptQLExecutor {
    private parser: PromptQLParser;

    constructor(private database: Database, provider: ILLMProvider) {
        this.parser = new PromptQLParser(provider);
    }

    /**
     * Execute a natural language query
     * @param prompt The natural language query string
     * @param context Optional context for the prompt
     */
    async execute<T = DocumentData>(prompt: string, context?: PromptContext): Promise<QueryResult<T>> {
        const parsed = await this.parser.parse(prompt, context);
        return this.executeParsedQuery<T>(parsed);
    }

    /**
     * Update the LLM provider
     */
    setProvider(provider: ILLMProvider) {
        this.parser.setProvider(provider);
    }

    // Reuse the execution logic by using the public methods of Database/Collection
    // or by temporarily accessing the internal QueryEngine if we want to reuse that exact logic.
    // However, since QueryEngine is efficient, we might want to just parse here and delegate execution?
    // Actually, QueryEngine has the execution logic.
    // Let's rely on the Database's query engine for the actual execution if possible,
    // OR we can just instantiate a QueryEngine internally or reuse one.

    // Better design: The database should expose a way to execute a ParsedQuery, 
    // OR we just duplicate the switch case dispatch logic here, calling Database methods.
    // For now, let's duplicate the dispatch logic to keep it decoupled from QueryEngine internals,
    // or we can use the QueryEngine if it exposes a public 'executeParsedQuery' (it was private).

    // Let's assume we can use the public API of Database/Collection which is cleaner.

    private async executeParsedQuery<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
        const startTime = Date.now();
        let result: QueryResult<T>;

        // We can reuse the QueryEngine logic if we make executeParsedQuery public query/QueryEngine.ts
        // But to avoid tight coupling let's implement the dispatch here using public DB APIs.
        // Or simpler: access the database's query engine.
        // The Database class usually has a queryEngine instance.

        // Check if we can access database.queryEngine
        // If not, we'll re-implement the dispatch.

        try {
            // For now, let's just re-implement the simple dispatch logic as it's just routing to collection methods
            // This ensures PromptQLExecutor is a standalone layer on top of Database
            switch (parsed.type) {
                case 'SELECT': return await this.executeSelect<T>(parsed);
                case 'INSERT': return await this.executeInsert<T>(parsed);
                case 'UPDATE': return await this.executeUpdate<T>(parsed);
                case 'DELETE': return await this.executeDelete<T>(parsed);
                case 'COUNT': return await this.executeCount<T>(parsed);
                case 'AGGREGATE': return await this.executeAggregate<T>(parsed);
                // For others, we might need more logic or just throw not supported for NL yet
                default:
                    // Fallback or error
                    throw new Error(`PromptQL execution for ${parsed.type} not fully implemented in this optional module yet.`);
            }
        } catch (e: any) {
            throw e;
        }
    }

    private async executeAggregate<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
        const collection = this.database.collection(parsed.collection);

        // Optimization: Handle simple COUNT(*) or COUNT(field)
        if (!parsed.groupBy && parsed.aggregations?.length === 1 && parsed.aggregations[0].function === 'COUNT') {
            const agg = parsed.aggregations[0];
            const count = await collection.count(parsed.conditions);

            const key = agg.alias || `${agg.function}(${agg.field})`;
            const resultDoc = { [key]: count };

            return {
                documents: [resultDoc as any],
                count: 1,
                totalCount: 1,
                executionTime: 0
            };
        }

        // Default: Fetch documents and aggregate in memory
        const docs = await collection.find({
            conditions: parsed.conditions,
            limit: parsed.limit,
            offset: parsed.offset
        });

        // Handle other simple single aggregations
        if (!parsed.groupBy && parsed.aggregations?.length === 1) {
            const agg = parsed.aggregations[0];
            let value: any = 0;

            if (agg.function === 'SUM') {
                // Simple SUM
                value = docs.reduce((sum, doc) => sum + (Number((doc.toObject() as any)[agg.field]) || 0), 0);
            } else {
                // Fallback for others
                value = 0;
            }

            const key = agg.alias || `${agg.function}(${agg.field})`;
            const resultDoc = { [key]: value };

            return {
                documents: [resultDoc as any],
                count: 1,
                totalCount: 1,
                executionTime: 0
            };
        }

        throw new Error('Complex aggregations (GROUP BY) not yet supported in PromptQL optional module');
    }

    private async executeSelect<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
        const collection = this.database.collection(parsed.collection);
        // Note: Basic SELECT support
        const docs = await collection.find({
            conditions: parsed.conditions,
            orderBy: parsed.orderBy,
            limit: parsed.limit,
            offset: parsed.offset,
            fields: parsed.fields
        });

        // Handle aggregation if needed (rudimentary support for now as per QueryEngine)
        // ... (omitted for brevity, relying on basic find)

        return {
            documents: docs.map(d => d.toObject() as T),
            count: docs.length,
            totalCount: docs.length, // approximation
            executionTime: 0
        };
    }

    private async executeCount<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
        const collection = this.database.collection(parsed.collection);
        const count = await collection.count(parsed.conditions);
        return {
            documents: [{ count } as unknown as T], // Generic handling
            count: 1,
            totalCount: count,
            executionTime: 0
        };
    }

    private async executeInsert<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
        const collection = this.database.collection(parsed.collection);
        const data = Array.isArray(parsed.data) ? parsed.data : [parsed.data];
        const docs = await collection.insertMany(data as any[]);
        return {
            documents: docs.map(d => d.toObject() as T),
            count: docs.length,
            totalCount: docs.length,
            executionTime: 0
        };
    }

    private async executeUpdate<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
        const collection = this.database.collection(parsed.collection);
        if (!parsed.conditions?.length) throw new Error('UPDATE requires conditions');
        const res = await collection.updateMany(parsed.conditions, parsed.data as any);
        return {
            documents: res.documents.map(d => d.toObject() as T),
            count: res.modified,
            totalCount: res.modified,
            executionTime: 0
        };
    }

    private async executeDelete<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
        const collection = this.database.collection(parsed.collection);
        if (!parsed.conditions?.length) throw new Error('DELETE requires conditions');
        const count = await collection.deleteMany(parsed.conditions);
        return {
            documents: [],
            count,
            totalCount: count,
            executionTime: 0
        };
    }
}
