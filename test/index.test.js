const test = require('brittle')
const duckdb = require('../binding')
const Duckdb = require('../index')

test('hello Duckdb', async (t) => {
    try {
        var db = await Duckdb.create('test.db')
        t.ok(db)
        t.ok(db.pathname === 'test.db')
    } catch (error) {
        console.error('unexpected Duckdb create error', error)
    }

    try {
        const result = await db.query('SELECT 1 + 1 AS value')
        console.log('1 + 1 result', result)
        t.ok(Array.isArray(result), 'result should be an array')
        t.is(result.length, 1, 'result array should have one row')
        t.is(result[0].value, 2, 'value should be 2')
    } catch (error) {
        console.error('unexpected Duckdb query error', error)
    }

    try {
        const result = await db.query('SELECT \'hello\' AS text')
        console.log('text result', result)
        t.ok(Array.isArray(result), 'stringResult should be an array')
        t.is(result[0].text, 'hello', 'text should be "hello"')

    } catch (error) {
        console.error('unexpected Duckdb query error', error)
    }

    try {
        await db.close()
    } catch (error) {
        console.error('unexpected Duckdb create error', error)
    }
})

test('hello bindings', async (t) => {
    try {
        const db = await duckdb.open('test.db')
        t.ok(db)

        await duckdb.connect(db)
        const result = await duckdb.query(db, 'SELECT 1 + 1 AS value')
        console.log('bindings 1 + 1 result', result)
        t.ok(Array.isArray(result), 'result should be an array')
        t.is(result.length, 1, 'result array should have one row')
        t.is(result[0].value, 2, 'value should be 2')

        const stringResult = await duckdb.query(db, 'SELECT \'hello\' AS text')
        console.log('bindings text result', stringResult)
        t.ok(Array.isArray(stringResult), 'stringResult should be an array')
        t.is(stringResult[0].text, 'hello', 'text should be "hello"')

        await duckdb.close(db)
    } catch (error) {
        console.error('unexpected binding error', error)
    }
})
