const pool = require('./db');

// Utility function to execute queries
async function executeQuery(query, params = []) {
    try {
        const boundQuery = pool.query.bind(pool);
        return await boundQuery(query, params);
    } catch (err) {
        console.error('Error executing query:', err.message);
        throw err;
    }
}

// Load token ratings considering both token and chain
async function loadTokenRatings() {
    try {
        const query = 'SELECT token, chain, tier FROM ratings.tokens;';
        console.log('Calling pool.query for token ratings...');
        const tokens = await executeQuery(query);

        console.log('Tokens from ratings.tokens:', tokens.rows);

        const tokenRatings = {};
        tokens.rows.forEach(token => {
            if (!token.token || !token.chain) {
                console.warn('Invalid token entry in ratings.tokens:', token);
                return;
            }
            const key = `${token.token.trim().toUpperCase()}-${token.chain.trim().toUpperCase()}`;
            tokenRatings[key] = parseFloat(token.tier);
        });

        console.log('Token Ratings Dictionary:', tokenRatings);
        return tokenRatings;
    } catch (error) {
        console.error('Error loading token ratings:', error.message);
        throw error;
    }
}

// Calculate and update strategy ratings for pools
async function calculateStrategyRatings() {
    try {
        const tokenRatings = await loadTokenRatings();
        console.log('Token Ratings:', tokenRatings); // Log the token ratings dictionary

        const query = 'SELECT id, token1, token2, chain, protocol, roi, rating FROM ratings.pools;';
        console.log('Calling pool.query for pool data...');
        const result = await executeQuery(query);
        const pools = result.rows;

        for (const pool of pools) {
            const token1Key = `${pool.token1.trim().toUpperCase()}-${pool.chain.trim().toUpperCase()}`;
            const token2Key = `${pool.token2.trim().toUpperCase()}-${pool.chain.trim().toUpperCase()}`;

            const token1Rating = tokenRatings[token1Key] || 0;
            const token2Rating = tokenRatings[token2Key] || 0;

            console.log(`PoolID: ${pool.id}, Token1: ${pool.token1}, Token2: ${pool.token2}, Chain: ${pool.chain}`);
            console.log(`Token1Rating: ${token1Rating}, Token2Rating: ${token2Rating}`);

            if (token1Rating === 0 || token2Rating === 0) {
                console.warn(
                    `Skipping PoolID ${pool.id}: Missing or zero rating for Token1 (${pool.token1}, Chain: ${pool.chain}) or Token2 (${pool.token2}, Chain: ${pool.chain})`
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
                    `Skipping PoolID ${pool.id}: Invalid averageTokenRating (${averageTokenRating}) or rating (${rating})`
                );
                continue;
            }

            const updateQuery = `
                UPDATE ratings.pools
                SET strategy_rating = $1
                WHERE id = $2;
            `;
            console.log(`Updating strategy rating for PoolID ${pool.id} with value: ${strategyRating}`);
            await executeQuery(updateQuery, [strategyRating, pool.id]);
        }

        console.log('Strategy Ratings updated successfully in the database.');
    } catch (error) {
        console.error('Error calculating strategy ratings:', error.message);
    }
}

// Execute the script
calculateStrategyRatings().catch(console.error);