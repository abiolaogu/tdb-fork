import { BaseLLMProvider } from './BaseLLMProvider';
import { CompletionResponse, PromptContext } from '../types';

export class DeepSeekProvider extends BaseLLMProvider {
    constructor() {
        super('deepseek');
    }

    async generate(prompt: string, context?: PromptContext): Promise<CompletionResponse> {
        if (!this.config.apiKey) {
            throw new Error('DeepSeek API key not configured');
        }

        const model = this.config.model || 'deepseek-chat';
        const url = this.config.baseUrl || 'https://api.deepseek.com/chat/completions';

        const systemPrompt = this.buildSystemPrompt(context);

        const messageHistory: any[] = (context?.history || []).map(msg => ({
            role: msg.role,
            content: msg.content
        }));

        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.config.apiKey}`
            },
            body: JSON.stringify({
                model,
                messages: [
                    { role: 'system', content: systemPrompt },
                    ...messageHistory,
                    { role: 'user', content: prompt }
                ],
                temperature: this.config.temperature ?? 0,
                max_tokens: this.config.maxTokens ?? 1000,
                stream: false
            })
        });

        if (!response.ok) {
            const error = await response.text();
            throw new Error(`DeepSeek API request failed: ${error}`);
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
