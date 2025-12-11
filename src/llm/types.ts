export interface LLMConfig {
    apiKey?: string;
    model?: string;
    baseUrl?: string;
    temperature?: number;
    maxTokens?: number;
    timeout?: number;
}

export interface PromptContext {
    schema?: string; // Serialized schema info (tables, fields)
    dialect?: string; // e.g. "LQL"
    history?: { role: 'user' | 'assistant'; content: string }[];
}

export interface CompletionResponse {
    content: string;
    usage?: {
        promptTokens: number;
        completionTokens: number;
        totalTokens: number;
    };
    raw?: any;
}

export interface ILLMProvider {
    name: string;

    /**
     * Configure the provider
     */
    configure(config: LLMConfig): void;

    /**
     * Generate a completion for a given prompt and context
     * @param prompt User's natural language query
     * @param context Schema and conversation context
     */
    generate(prompt: string, context?: PromptContext): Promise<CompletionResponse>;

    /**
     * Check if the provider is properly configured and ready
     */
    healthCheck(): Promise<boolean>;
}
