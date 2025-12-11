import { BaseLLMProvider } from './BaseLLMProvider';
import { CompletionResponse, PromptContext } from '../types';

export class LlamaProvider extends BaseLLMProvider {
    constructor() {
        super('llama');
    }

    async generate(prompt: string, context?: PromptContext): Promise<CompletionResponse> {
        // Default to local Ollama instance
        const url = this.config.baseUrl || 'http://localhost:11434/api/generate';
        const model = this.config.model || 'llama3';

        const systemPrompt = this.buildSystemPrompt(context);
        const fullPrompt = `${systemPrompt}\n\nUser Query: ${prompt}`;

        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                model,
                prompt: fullPrompt,
                stream: false,
                options: {
                    temperature: this.config.temperature ?? 0,
                    num_predict: this.config.maxTokens ?? 1000
                }
            })
        });

        if (!response.ok) {
            const error = await response.text();
            throw new Error(`Llama/Ollama request failed: ${error}`);
        }

        const data = await response.json() as any;

        return {
            content: data.response ? data.response.trim() : '',
            usage: {
                promptTokens: data.prompt_eval_count || 0,
                completionTokens: data.eval_count || 0,
                totalTokens: (data.prompt_eval_count || 0) + (data.eval_count || 0)
            },
            raw: data
        };
    }
}
