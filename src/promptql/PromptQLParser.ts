import { ParsedQuery, QuerySyntaxError } from '../types';
import { ILLMProvider, PromptContext } from '../llm/types';
import { LQLParser } from '../query/parsers/LQLParser';

export class PromptQLParser {
    private lqlParser: LQLParser;

    constructor(private llmProvider: ILLMProvider) {
        this.lqlParser = new LQLParser();
    }

    /**
     * Parse a natural language prompt into a ParsedQuery structure
     * 1. Uses LLM to translate NL -> LQL string
     * 2. Uses LQLParser to parse LQL string -> ParsedQuery object
     */
    async parse(prompt: string, context?: PromptContext): Promise<ParsedQuery> {
        try {
            // 1. Generate LQL from Natural Language
            const response = await this.llmProvider.generate(prompt, context);
            const lqlQuery = this.extractLQL(response.content);

            if (lqlQuery.startsWith('ERROR:')) {
                throw new QuerySyntaxError(`LLM could not generate query: ${lqlQuery}`);
            }

            // 2. Parse LQL into AST
            return this.lqlParser.parse(lqlQuery);
        } catch (error: any) {
            throw new QuerySyntaxError(`PromptQL Error: ${error.message}`);
        }
    }

    /**
     * Update the LLM provider
     */
    setProvider(provider: ILLMProvider) {
        this.llmProvider = provider;
    }

    private extractLQL(content: string): string {
        // Remove simple markdown code blocks if present
        let clean = content.replace(/```lql/gi, '').replace(/```/g, '').trim();

        // Remove trailing semicolons for cleaner parsing if needed (parser handles it usually)
        if (clean.endsWith(';')) clean = clean.slice(0, -1);

        return clean;
    }
}
