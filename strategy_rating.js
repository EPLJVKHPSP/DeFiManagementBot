const pool = require('./db');

async function executeQuery(query, params = []) {
    try {
        const boundQuery = pool.query.bind(pool);
        return await boundQuery(query, params);
    } catch (err) {
        console.error('Error executing query:', err.message);
        throw err;
    }
}

async function loadTokenRatings() {
    try {
        const query = 'SELECT token, tier FROM ratings.tokens;';
        console.log('Calling pool.query for token ratings...');
        const tokens = await executeQuery(query);

        console.log('Tokens from ratings.tokens:', tokens.rows);

        const tokenRatings = {};
        tokens.rows.forEach(token => {
            if (!token.token) {
                console.warn('Invalid token entry in ratings.tokens:', token);
                return;
            }
            tokenRatings[token.token.trim().toUpperCase()] = parseFloat(token.tier);
        });

        console.log('Token Ratings Dictionary:', tokenRatings);
        return tokenRatings;
    } catch (error) {
        console.error('Error loading token ratings:', error.message);
        throw error;
    }
}

async function calculateStrategyRatings() {
    try {
        const tokenRatings = await loadTokenRatings();
        console.log('Token Ratings:', tokenRatings); // Log the token ratings dictionary

        const query = 'SELECT * FROM ratings.pools;';
        console.log('Calling pool.query for pool data...');
        const result = await executeQuery(query);
        const pools = result.rows;

        for (const pool of pools) {
            const token1Rating = tokenRatings[pool.token1.trim().toUpperCase()] || 0;
            const token2Rating = tokenRatings[pool.token2.trim().toUpperCase()] || 0;

            console.log(`PoolID: ${pool.pool_id}, Token1: ${pool.token1}, Token2: ${pool.token2}`);
            console.log(`Token1Rating: ${token1Rating}, Token2Rating: ${token2Rating}`);

            if (token1Rating === 0 || token2Rating === 0) {
                console.warn(
                    `Skipping PoolID ${pool.pool_id}: Missing or zero rating for Token1 (${pool.token1}) or Token2 (${pool.token2})`
                );
                continue;
            }

            const averageTokenRating = (token1Rating + token2Rating) / 2;
            const rating = parseFloat(pool.rating) || 0;

            let strategyRating = null; // Default to null
            if (averageTokenRating > 0 && rating > 0) {
                strategyRating = rating / averageTokenRating / 10000000;
            } else {
                console.warn(
                    `Skipping PoolID ${pool.pool_id}: Invalid averageTokenRating (${averageTokenRating}) or rating (${rating})`
                );
                continue;
            }

            const updateQuery = `
                UPDATE ratings.pools
                SET strategy_rating = $1
                WHERE pool_id = $2;
            `;
            console.log(`Updating strategy rating for PoolID ${pool.pool_id} with value: ${strategyRating}`);
            await executeQuery(updateQuery, [strategyRating, pool.pool_id]);
        }

        console.log('Strategy Ratings updated successfully in the database.');
    } catch (error) {
        console.error('Error calculating strategy ratings:', error.message);
    }
}

// Execute the script
calculateStrategyRatings().catch(console.error);