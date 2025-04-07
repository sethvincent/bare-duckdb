const duckdb = require('./binding')
const Duckdb = require('./index')

async function main () {
    try {
        var db = await Duckdb.create('test.db')
    } catch (error) {
        console.error('unexpected Duckdb create error', error)
    }

    try {
        const result = await db.query('SELECT 3 + 3 AS value')
        console.log('3 + 3 result', result, result[0].value === 6)
    } catch (error) {
        console.error('unexpected Duckdb query error', error)
    }

    try {
        const result = await db.query('SELECT 1 + 1 AS value')
        console.log('1 + 1 result', result, result[0].value === 2)
    } catch (error) {
        console.error('unexpected Duckdb query error', error)
    }

    try {
        const result = await db.query('SELECT 2 + 2 AS value')
        console.log('2 + 2 result', result, result[0].value === 4)
    } catch (error) {
        console.error('unexpected Duckdb query error', error)
    }

    try {
        const result = await db.query('SELECT 4 + 4 AS value')
        console.log('4 + 4 result', result, result[0].value === 8)
    } catch (error) {
        console.error('unexpected Duckdb query error', error)
    }

    try {
        const result = await db.query('SELECT \'hello\' AS text')
        console.log('text result', result)
    } catch (error) {
        console.error('unexpected Duckdb query error', error)
    }

    try {
        await db.close()
    } catch (error) {
        console.error('unexpected Duckdb create error', error)
    }

    try {
        const db = await duckdb.open('test.db')
        await duckdb.connect(db)

        {
            const result = await duckdb.query(db, 'SELECT 1 + 1 AS value')
            console.log('bindings 1 + 1 result', result)
        }

        {
            const result = await duckdb.query(db, 'SELECT 2 + 2 AS value')
            console.log('bindings 2 + 2 result', result)
        }

        {
            const result = await duckdb.query(db, 'SELECT 4 + 4 AS value')
            console.log('bindings 4 + 4 result', result)
        }

        {
            const result = await duckdb.query(db, 'SELECT \'hello\' AS text')
            console.log('bindings text result', result)
        }

        await duckdb.close(db)
    } catch (error) {
        console.error('unexpected binding error', error)
    }
}

main().catch(console.error)
