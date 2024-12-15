const { Pool } = require('pg');

// Create a connection pool for PostgreSQL
const pool = new Pool({
    user: 'metabase_admin',
    host: '147.78.130.225',
    database: 'postgres',
    password: 'wFRH@Uuerfhq@I23R3EJU',
    port: 5432, // Default PostgreSQL port
});

module.exports = pool;