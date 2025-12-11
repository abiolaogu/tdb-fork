import { BaseLLMProvider } from './BaseLLMProvider';
import { CompletionResponse, PromptContext } from '../types';

export class GeminiProvider extends BaseLLMProvider {
    constructor() {
        super('gemini');
    }

    async generate(prompt: string, context?: PromptContext): Promise<CompletionResponse> {
        if (!this.config.apiKey) {
            throw new Error('Gemini API key not configured');
        }

        const model = this.config.model || 'gemini-pro';
        const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${this.config.apiKey}`;

        const systemPrompt = this.buildSystemPrompt(context);

        // Gemini handles system prompts differently or as first turn, simpler to just prepend for now
        const fullPrompt = `${systemPrompt}\n\nUser Query: ${prompt}`;

        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                contents: [{ parts: [{ text: fullPrompt }] }],
                generationConfig: {
                    temperature: this.config.temperature ?? 0,
                    maxOutputTokens: this.config.maxTokens ?? 1000
                }
            })
        });

        if (!response.ok) {
            const error = await response.text();
            throw new Error(`Gemini API request failed: ${error}`);
        }

        const data = await response.json() as any;
        const content = data.candidates?.[0]?.content?.parts?.[0]?.text || '';

        return {
            content: content.trim(),
            // Gemini doesn't always return token usage in simple response, defaulting to 0
            usage: {
                promptTokens: 0,
                completionTokens: 0,
                totalTokens: 0
            },
            raw: data
        };
    }
}
