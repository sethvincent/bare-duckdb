const test = require('brittle')
const duckdb = require('../binding')
const Duckdb = require('../index')

test('hello Duckdb', async (t) => {
    const db = await Duckdb.create('test.db')
    t.ok(db)
    t.ok(db.pathname === 'test.db')
    t.ok(db.connection)

    {
        const result = await db.query('SELECT 1 + 1 AS value')
        t.ok(Array.isArray(result), 'result should be an array')
        t.is(result.length, 1, 'result array should have one row')
        t.is(result[0].value, 2, 'value in the first row should be 2')
    }

    {
        const result = await db.query('SELECT \'hello\' AS text')
        t.ok(Array.isArray(result), 'stringResult should be an array')
        t.is(result[0].text, 'hello', 'text in the first row should be "hello"')

    }

    await db.close()
})

test('hello bindings', async (t) => {
        const db = await duckdb.open('test.db')
        t.ok(db)

        const connection = await duckdb.connect(db)
        t.ok(connection)

        const result = await duckdb.query(db, 'SELECT 1 + 1 AS value')
        t.ok(Array.isArray(result), 'result should be an array')
        t.is(result.length, 1, 'result array should have one row')
        t.is(result[0].value, 2, 'value in the first row should be 2')

        const stringResult = await duckdb.query(db, 'SELECT \'hello\' AS text')
        t.ok(Array.isArray(stringResult), 'stringResult should be an array')
        t.is(stringResult[0].text, 'hello', 'text in the first row should be "hello"')

        await duckdb.disconnect(connection)
        await duckdb.close(db)
})
