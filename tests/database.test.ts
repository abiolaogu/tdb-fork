/**
 * TDB+ Database Tests
 */

import { Database } from '../src/core/Database';
import { Collection } from '../src/core/Collection';

describe('Database', () => {
  let db: Database;

  beforeEach(async () => {
    db = Database.create('test_db');
    await db.open();
  });

  afterEach(async () => {
    await db.close();
  });

  describe('Basic Operations', () => {
    it('should create and open database', () => {
      expect(db.getName()).toBe('test_db');
    });

    it('should create collections', () => {
      const users = db.collection('users');
      expect(users).toBeInstanceOf(Collection);
      expect(db.hasCollection('users')).toBe(true);
    });

    it('should list collection names', () => {
      db.collection('users');
      db.collection('orders');
      const names = db.getCollectionNames();
      expect(names).toContain('users');
      expect(names).toContain('orders');
    });

    it('should drop collections', async () => {
      db.collection('temp');
      expect(db.hasCollection('temp')).toBe(true);
      await db.dropCollection('temp');
      expect(db.hasCollection('temp')).toBe(false);
    });
  });

  describe('TQL Queries', () => {
    it('should insert data with TQL', async () => {
      const result = await db.tql(
        `INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 28)`
      );
      expect(result.count).toBe(1);
      expect(result.documents[0]).toMatchObject({
        name: 'Alice',
        email: 'alice@example.com',
        age: 28,
      });
    });

    it('should select data with TQL', async () => {
      await db.tql(`INSERT INTO users (name, age) VALUES ('Alice', 28)`);
      await db.tql(`INSERT INTO users (name, age) VALUES ('Bob', 32)`);

      const result = await db.tql(`SELECT * FROM users WHERE age > 25`);
      expect(result.count).toBe(2);
    });

    it.skip('should select with ORDER BY and LIMIT', async () => {
      await db.tql(`INSERT INTO users (name, age) VALUES ('Alice', 28)`);
      await db.tql(`INSERT INTO users (name, age) VALUES ('Bob', 32)`);
      await db.tql(`INSERT INTO users (name, age) VALUES ('Charlie', 25)`);

      const result = await db.tql(`SELECT * FROM users ORDER BY age DESC LIMIT 2`);
      expect(result.count).toBe(2);
      expect(result.documents[0].name).toBe('Bob');
      expect(result.documents[1].name).toBe('Alice');
    });

    it.skip('should update data with TQL', async () => {
      await db.tql(`INSERT INTO users (name, status) VALUES ('Alice', 'inactive')`);

      const result = await db.tql(
        `UPDATE users SET status = 'active' WHERE name = 'Alice'`
      );
      expect(result.count).toBe(1);

      const check = await db.tql(`SELECT * FROM users WHERE name = 'Alice'`);
      expect(check.documents[0].status).toBe('active');
    });

    it.skip('should delete data with TQL', async () => {
      await db.tql(`INSERT INTO users (name, inactive) VALUES ('Alice', true)`);
      await db.tql(`INSERT INTO users (name, inactive) VALUES ('Bob', false)`);

      const result = await db.tql(`DELETE FROM users WHERE inactive = true`);
      expect(result.count).toBe(1);

      const remaining = await db.tql(`SELECT * FROM users`);
      expect(remaining.count).toBe(1);
      expect(remaining.documents[0].name).toBe('Bob');
    });
  });

  describe('NQL Queries', () => {
    it('should insert data with NQL', async () => {
      const result = await db.nql(`add to users name "Alice", email "alice@example.com", age 28`);
      expect(result.count).toBe(1);
    });

    it('should find data with NQL', async () => {
      await db.nql(`add to users name "Alice", age 28`);
      await db.nql(`add to users name "Bob", age 32`);

      const result = await db.nql(`find all users where age is greater than 25`);
      expect(result.count).toBe(2);
    });

    it('should find with natural conditions', async () => {
      await db.nql(`add to users name "Alice", age 28`);
      await db.nql(`add to users name "Bob", age 32`);
      await db.nql(`add to users name "Charlie", age 25`);

      const result = await db.nql(`get first 2 users sorted by age descending`);
      expect(result.count).toBe(2);
      expect(result.documents[0].name).toBe('Bob');
    });

    it('should count with NQL', async () => {
      await db.nql(`add to users name "Alice", status "active"`);
      await db.nql(`add to users name "Bob", status "active"`);
      await db.nql(`add to users name "Charlie", status "inactive"`);

      const result = await db.nql(`count users where status equals "active"`);
      expect(result.totalCount).toBe(2);
    });
  });

  describe('JQL Queries', () => {
    it('should insert data with JQL', async () => {
      const result = await db.jql(
        `{ "insert": "users", "documents": [{ "name": "Alice", "age": 28 }] }`
      );
      expect(result.count).toBe(1);
    });

    it('should find data with JQL', async () => {
      await db.jql(`{ "insert": "users", "documents": [{ "name": "Alice", "age": 28 }] }`);
      await db.jql(`{ "insert": "users", "documents": [{ "name": "Bob", "age": 32 }] }`);

      const result = await db.jql(
        `{ "find": "users", "filter": { "age": { "$gt": 25 } } }`
      );
      expect(result.count).toBe(2);
    });

    it('should update data with JQL', async () => {
      await db.jql(`{ "insert": "users", "documents": [{ "name": "Alice", "status": "inactive" }] }`);

      const result = await db.jql(
        `{ "update": "users", "filter": { "name": "Alice" }, "set": { "status": "active" } }`
      );
      expect(result.count).toBe(1);
    });

    it('should support complex filters with JQL', async () => {
      await db.jql(`{ "insert": "products", "documents": [
        { "name": "Widget", "price": 29.99, "category": "tools" },
        { "name": "Gadget", "price": 49.99, "category": "electronics" },
        { "name": "Gizmo", "price": 99.99, "category": "electronics" }
      ] }`);

      const result = await db.jql(`{
        "find": "products",
        "filter": {
          "$and": [
            { "category": "electronics" },
            { "price": { "$lt": 60 } }
          ]
        }
      }`);
      expect(result.count).toBe(1);
      expect(result.documents[0].name).toBe('Gadget');
    });
  });

  describe('Transactions', () => {
    it('should commit successful transactions', async () => {
      await db.transaction(async (tx) => {
        await tx.collection('users').insert({ name: 'Alice' });
        await tx.collection('users').insert({ name: 'Bob' });
      });

      const result = await db.tql(`SELECT * FROM users`);
      expect(result.count).toBe(2);
    });

    it('should rollback failed transactions', async () => {
      try {
        await db.transaction(async (tx) => {
          await tx.collection('users').insert({ name: 'Alice' });
          throw new Error('Simulated failure');
        });
      } catch {
        // Expected
      }

      const result = await db.tql(`SELECT * FROM users`);
      expect(result.count).toBe(0);
    });
  });

  describe('Events', () => {
    it('should emit document:created events', async () => {
      const events: any[] = [];
      db.on('document:created', (e) => events.push(e));

      await db.tql(`INSERT INTO users (name) VALUES ('Alice')`);

      expect(events.length).toBe(1);
      expect(events[0].data.collection).toBe('users');
    });

    it('should emit query:executed events', async () => {
      const events: any[] = [];
      db.on('query:executed', (e) => events.push(e));

      await db.tql(`SELECT * FROM users`);

      expect(events.length).toBe(1);
      expect(events[0].data.language).toBe('tql');
    });
  });
});

describe('Collection', () => {
  let db: Database;
  let users: Collection;

  beforeEach(async () => {
    db = Database.create('test_db');
    await db.open();
    users = db.collection('users');
  });

  afterEach(async () => {
    await db.close();
  });

  describe('CRUD Operations', () => {
    it('should insert a document', async () => {
      const doc = await users.insert({ name: 'Alice', age: 28 });
      expect(doc.id).toBeDefined();
      expect(doc.get('name')).toBe('Alice');
    });

    it('should insert many documents', async () => {
      const docs = await users.insertMany([
        { name: 'Alice' },
        { name: 'Bob' },
        { name: 'Charlie' },
      ]);
      expect(docs.length).toBe(3);
    });

    it('should find by ID', async () => {
      const inserted = await users.insert({ name: 'Alice' });
      const found = await users.findById(inserted.id);
      expect(found).not.toBeNull();
      expect(found!.get('name')).toBe('Alice');
    });

    it('should return null for non-existent ID', async () => {
      const found = await users.findById('non-existent');
      expect(found).toBeNull();
    });

    it('should find with conditions', async () => {
      await users.insert({ name: 'Alice', age: 28 });
      await users.insert({ name: 'Bob', age: 32 });
      await users.insert({ name: 'Charlie', age: 25 });

      const results = await users.find({
        conditions: [{ field: 'age', operator: '>', value: 26 }],
      });
      expect(results.length).toBe(2);
    });

    it('should find one', async () => {
      await users.insert({ name: 'Alice' });
      await users.insert({ name: 'Bob' });

      const result = await users.findOne();
      expect(result).not.toBeNull();
    });

    it('should update by ID', async () => {
      const doc = await users.insert({ name: 'Alice', status: 'inactive' });
      await users.updateById(doc.id, { status: 'active' });

      const updated = await users.findById(doc.id);
      expect(updated!.get('status')).toBe('active');
    });

    it('should delete by ID', async () => {
      const doc = await users.insert({ name: 'Alice' });
      const deleted = await users.deleteById(doc.id);
      expect(deleted).toBe(true);

      const found = await users.findById(doc.id);
      expect(found).toBeNull();
    });

    it('should count documents', async () => {
      await users.insertMany([
        { status: 'active' },
        { status: 'active' },
        { status: 'inactive' },
      ]);

      const total = await users.count();
      expect(total).toBe(3);

      const active = await users.count([{ field: 'status', operator: '=', value: 'active' }]);
      expect(active).toBe(2);
    });
  });

  describe('Query Operators', () => {
    beforeEach(async () => {
      await users.insertMany([
        { name: 'Alice', age: 28, email: 'alice@example.com' },
        { name: 'Bob', age: 32, email: 'bob@test.com' },
        { name: 'Charlie', age: 25, email: 'charlie@example.com' },
        { name: 'Diana', age: 30, email: 'diana@test.com' },
      ]);
    });

    it('should support LIKE operator', async () => {
      const results = await users.find({
        conditions: [{ field: 'email', operator: 'LIKE', value: '%example.com' }],
      });
      expect(results.length).toBe(2);
    });

    it('should support IN operator', async () => {
      const results = await users.find({
        conditions: [{ field: 'age', operator: 'IN', value: [28, 30] }],
      });
      expect(results.length).toBe(2);
    });

    it('should support BETWEEN operator', async () => {
      const results = await users.find({
        conditions: [{ field: 'age', operator: 'BETWEEN', value: [26, 31] }],
      });
      expect(results.length).toBe(2);
    });

    it('should support CONTAINS operator', async () => {
      const results = await users.find({
        conditions: [{ field: 'email', operator: 'CONTAINS', value: 'example' }],
      });
      expect(results.length).toBe(2);
    });
  });

  describe('Ordering and Pagination', () => {
    beforeEach(async () => {
      await users.insertMany([
        { name: 'Alice', age: 28 },
        { name: 'Bob', age: 32 },
        { name: 'Charlie', age: 25 },
        { name: 'Diana', age: 30 },
      ]);
    });

    it('should order by field ascending', async () => {
      const results = await users.find({
        orderBy: [{ field: 'age', direction: 'ASC' }],
      });
      expect(results[0].get('name')).toBe('Charlie');
      expect(results[3].get('name')).toBe('Bob');
    });

    it('should order by field descending', async () => {
      const results = await users.find({
        orderBy: [{ field: 'age', direction: 'DESC' }],
      });
      expect(results[0].get('name')).toBe('Bob');
      expect(results[3].get('name')).toBe('Charlie');
    });

    it('should limit results', async () => {
      const results = await users.find({ limit: 2 });
      expect(results.length).toBe(2);
    });

    it('should offset results', async () => {
      const results = await users.find({
        orderBy: [{ field: 'age', direction: 'ASC' }],
        offset: 2,
      });
      expect(results.length).toBe(2);
      expect(results[0].get('name')).toBe('Diana');
    });
  });
});
