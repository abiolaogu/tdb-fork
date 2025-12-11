import { Database } from '../../src';
import { expect } from 'chai';

describe('Storage Tiering Integration', () => {
    let db: Database;
    const TEST_COLLECTION = 'tiering_test';

    before(async () => {
        // Assumes server is running at localhost:8080
        // If not, this test might need a teardown/setup of a docker container or process
        db = new Database('http://localhost:8080'); // Adjust URL if needed
    });

    // Note: This test assumes proper server configuration (luma.yml loaded with tiering).

    it('should accept writes and reads with tiered storage enabled', async () => {
        const doc = { id: 'doc1', data: 'test data for tiering' };

        // Write
        const id = await db.collection(TEST_COLLECTION).insert(doc);
        expect(id).to.equal('doc1');

        // Read immediately (should be in RAM "Hot" tier)
        const result = await db.collection(TEST_COLLECTION).findById('doc1');
        expect(result.data).to.equal(doc.data);
    });

    it('should handle batch inserts potentially triggering flushes', async () => {
        const docs = Array.from({ length: 1000 }, (_, i) => ({
            id: `batch_${i}`,
            value: `data_${i}`.repeat(100)
        }));

        const result = await db.collection(TEST_COLLECTION).insertMany(docs);
        expect(result.count).to.equal(1000);

        // Verify a random doc
        const randomDoc = await db.collection(TEST_COLLECTION).findById('batch_500');
        expect(randomDoc.value).to.equal('data_500'.repeat(100));
    });

    // Ideally we would check stats here to see storage tiers usage
    // but stats API might be generic.
});
