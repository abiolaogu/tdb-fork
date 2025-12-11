import { ILLMProvider, LLMConfig, PromptContext, CompletionResponse } from '../types';

export abstract class BaseLLMProvider implements ILLMProvider {
    protected config: LLMConfig = {};

    constructor(protected providerName: string) { }

    get name(): string {
        return this.providerName;
    }

    configure(config: LLMConfig): void {
        this.config = { ...this.config, ...config };
    }

    abstract generate(prompt: string, context?: PromptContext): Promise<CompletionResponse>;

    async healthCheck(): Promise<boolean> {
        try {
            await this.generate('Hello', { dialect: 'text' });
            return true;
        } catch (e) {
            return false;
        }
    }

    protected buildSystemPrompt(context?: PromptContext): string {
        let systemPrompt = `You are an expert database assistant translating natural language into LQL (Luma Query Language).`;

        if (context?.dialect) {
            systemPrompt += `\nDialect: ${context.dialect}`;
        }

        if (context?.schema) {
            systemPrompt += `\n\nDatabase Schema:\n${context.schema}`;
        }

        systemPrompt += `\n\nRules:\n1. Return ONLY the valid LQL query string.\n2. Do not include markdown code blocks or explanations.\n3. If the request is impossible, return "ERROR: <reason>".`;

        return systemPrompt;
    }
}
