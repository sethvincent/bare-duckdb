const binding = require('./binding')

class Duckdb {
    static async create (pathname, options) {
        const db = new Duckdb(pathname, options)
        await db.open()
        await db.connect()
        return db
    }

    constructor (pathname, options = {}) {
        this.pathname = pathname
        this.options = options
    }

    async open () {
        this.duckdb = await binding.open(this.pathname)
    }

    async close () {
        // handles disconnect as part of `close(db)`
        await binding.close(this.duckdb)
    }

    async connect () {
        await binding.connect(this.duckdb)
    }

    disconnect () {
        binding.disconnect()
    }

    async query (sqlQuery) {
        return binding.query(this.duckdb, sqlQuery)
    }
}

module.exports = Duckdb
