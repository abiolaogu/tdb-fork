import { BaseLLMProvider } from './BaseLLMProvider';
import { CompletionResponse, PromptContext } from '../types';

export class OpenAIProvider extends BaseLLMProvider {
    constructor() {
        super('openai');
    }

    async generate(prompt: string, context?: PromptContext): Promise<CompletionResponse> {
        if (!this.config.apiKey) {
            throw new Error('OpenAI API key not configured');
        }

        const model = this.config.model || 'gpt-4o';
        const url = this.config.baseUrl || 'https://api.openai.com/v1/chat/completions';

        const systemPrompt = this.buildSystemPrompt(context);

        const messages = [
            { role: 'system', content: systemPrompt },
            ...(context?.history || []),
            { role: 'user', content: prompt }
        ];

        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.config.apiKey}`
            },
            body: JSON.stringify({
                model,
                messages,
                temperature: this.config.temperature ?? 0,
                max_tokens: this.config.maxTokens ?? 1000
            })
        });

        if (!response.ok) {
            const error = await response.text();
            throw new Error(`OpenAI API verification failed: ${error}`);
        }

        const data = await response.json() as any;
        const content = data.choices[0]?.message?.content || '';

        return {
            content: content.trim(),
            usage: {
                promptTokens: data.usage?.prompt_tokens || 0,
                completionTokens: data.usage?.completion_tokens || 0,
                totalTokens: data.usage?.total_tokens || 0
            },
            raw: data
        };
    }
}
