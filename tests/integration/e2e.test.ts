import { Database } from '../../src/native';
import { equal } from 'assert';

describe('E2E Native Tests', () => {
    it.skip('should insert and retrieve a document via native binding', async () => {
        console.log('Starting E2E Test...');

        // 1. Initialize Native DB
        console.log('Opening Database...');
        const db = await Database.open('./luma_test_data_e2e');

        // 2. Create Collection
        const users = db.collection('users');

        // 3. Insert Document
        console.log('Inserting document...');
        const doc = {
            // _id is optional now as we fixed rust side, but we provide it for verification
            name: 'Alice',
            email: 'alice@example.com',
            age: 30
        };

        const insertedId = await users.insert(JSON.stringify(doc));
        console.log(`Inserted document with ID: ${insertedId}`);

        // 4. Retrieve Document
        console.log('Retrieving document...');
        const fetchedJson = await users.get(insertedId);
        if (!fetchedJson) throw new Error('Document not found');

        const fetched = JSON.parse(fetchedJson);
        // If rust flattens it, name should be top level
        expect(fetched.name).toBe(doc.name);
        expect(fetched.age).toBe(doc.age);

        console.log('Verification successful!');

        // 5. Cleanup
        await db.close();
        console.log('Test Complete.');
    });
});
