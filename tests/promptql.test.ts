import { Database } from '../src/core/Database';
import { PromptQLExecutor } from '../src/promptql/PromptQLExecutor';
import { ILLMProvider, CompletionResponse, PromptContext } from '../src/llm/types';

// Mock LLM Provider
class MockLLMProvider implements ILLMProvider {
    name = 'mock-llm';

    configure(config: any) { }

    async generate(prompt: string, context?: PromptContext): Promise<CompletionResponse> {
        // Simple mock response logic
        let lql = '';
        if (prompt.includes('users sorted by name')) {
            lql = 'SELECT * FROM users ORDER BY name ASC';
        } else if (prompt.includes('count of orders')) {
            lql = 'SELECT COUNT(*) FROM orders';
        } else {
            lql = 'ERROR: I do not understand';
        }

        return {
            content: lql
        };
    }

    async healthCheck() { return true; }
}

describe('PromptQL Integration', () => {
    let db: Database;
    let executor: PromptQLExecutor;
    let mockProvider: MockLLMProvider;

    beforeEach(() => {
        // Mock Database minimal interface
        db = {
            collection: (name: string) => ({
                find: jest.fn().mockResolvedValue([{ data: { name: 'Alice' }, toObject: () => ({ name: 'Alice' }) }]),
                count: jest.fn().mockResolvedValue(10),
                insertMany: jest.fn(),
                updateMany: jest.fn().mockResolvedValue({ modified: 1, documents: [] }),
                deleteMany: jest.fn().mockResolvedValue(1),
            }),
            getIndexManager: () => ({
                findIndexForField: jest.fn().mockReturnValue(null)
            })
        } as unknown as Database;

        mockProvider = new MockLLMProvider();
        executor = new PromptQLExecutor(db, mockProvider);
    });

    test('should translate NL to LQL and execute SELECT', async () => {
        const result = await executor.execute('Show me all users sorted by name');

        expect(result).toBeDefined();
        // The mock DB returns 1 document
        expect(result.documents).toHaveLength(1);
        expect(result.documents[0]).toEqual({ name: 'Alice' });
    });

    test('should translate NL to LQL and execute COUNT', async () => {
        const result = await executor.execute('Get count of orders');

        expect(result).toBeDefined();
        // The executor uses collection.count() which returns 10 in our mock
        // But the result set contains 1 row (the aggregate)
        expect(result.count).toBe(1);
        expect(result.totalCount).toBe(1);

        // The value inside the document is the count
        expect(result.documents).toHaveLength(1);
        expect(result.documents[0]).toEqual({ 'COUNT(*)': 10 });
    });

    test('should handle LLM errors gracefully', async () => {
        await expect(executor.execute('Unknown gibberish'))
            .rejects
            .toThrow('LLM could not generate query');
    });
});
