import { BaseLLMProvider } from './BaseLLMProvider';
import { CompletionResponse, PromptContext } from '../types';

export class AnthropicProvider extends BaseLLMProvider {
    constructor() {
        super('anthropic');
    }

    async generate(prompt: string, context?: PromptContext): Promise<CompletionResponse> {
        if (!this.config.apiKey) {
            throw new Error('Anthropic API key not configured');
        }

        const model = this.config.model || 'claude-3-opus-20240229';
        const url = this.config.baseUrl || 'https://api.anthropic.com/v1/messages';

        const systemPrompt = this.buildSystemPrompt(context);

        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': this.config.apiKey,
                'anthropic-version': '2023-06-01'
            },
            body: JSON.stringify({
                model,
                system: systemPrompt,
                messages: [
                    ...(context?.history || []),
                    { role: 'user', content: prompt }
                ],
                temperature: this.config.temperature ?? 0,
                max_tokens: this.config.maxTokens ?? 1000
            })
        });

        if (!response.ok) {
            const error = await response.text();
            throw new Error(`Anthropic API request failed: ${error}`);
        }

        const data = await response.json() as any;
        const content = data.content?.[0]?.text || '';

        return {
            content: content.trim(),
            usage: {
                promptTokens: data.usage?.input_tokens || 0,
                completionTokens: data.usage?.output_tokens || 0,
                totalTokens: (data.usage?.input_tokens || 0) + (data.usage?.output_tokens || 0)
            },
            raw: data
        };
    }
}
