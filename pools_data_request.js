const axios = require('axios');
const pool = require('./db'); // Import the PostgreSQL connection pool

// This function uses the DeFiLlama API to fetch pool data
async function fetchPoolData(poolId) {
    const url = `https://yields.llama.fi/chart/${poolId}`;
    try {
        const response = await axios.get(url);
        const data = response.data.data;
        if (data && data.length > 0) {
            const lastDataPoint = data[data.length - 1];
            const firstDate = new Date(data[0].timestamp);
            const lastDate = new Date(lastDataPoint.timestamp);
            const tvlUsd = lastDataPoint.tvlUsd;
            const diffDays = Math.ceil((lastDate - firstDate) / (1000 * 60 * 60 * 24));
            const apy = lastDataPoint.apy || 'No APY Data'; // Default if APY is missing

            return {
                rating: (tvlUsd * diffDays).toString(), // Rating calculation
                apy: apy.toString(), // Storing APY value
            };
        }
        return { rating: 'No data available', apy: 'No APY Data' }; // Defaults if no data
    } catch (error) {
        console.error('Error fetching data for:', poolId, error);
        return {
            rating: 'Error fetching data',
            apy: 'Error',
        };
    }
}

// This function fetches pools from the database
async function fetchPoolsFromDatabase() {
    const query = `SELECT * FROM ratings.pools;`; // Adjust to match your database schema
    try {
        const result = await pool.query(query);
        return result.rows; // Return all rows from the pools table
    } catch (error) {
        console.error('Error fetching pools from database:', error.message);
        throw error;
    }
}

// This function updates the pool ratings in the database
async function updatePoolRatingsInDatabase(poolId, rating, apy) {
    const query = `
        UPDATE ratings.pools
        SET rating = $1, roi = $2
        WHERE pool_id = $3;
    `;
    try {
        await pool.query(query, [rating, apy, poolId]);
        console.log(`Successfully updated pool with ID: ${poolId}`);
    } catch (error) {
        console.error(`Error updating pool with ID ${poolId}:`, error.message);
        throw error;
    }
}

// Main function to update ratings
async function updateRatings() {
    try {
        // Step 1: Fetch all pools from the database
        const pools = await fetchPoolsFromDatabase();
        console.log('Pools successfully fetched from database');

        // Step 2: Update each pool with data from DeFiLlama
        const updatePromises = pools.map(async (pool) => {
            const poolData = await fetchPoolData(pool.pool_id);
            const rating = poolData.rating;
            const apy = poolData.apy;

            // Step 3: Update the pool data in the database
            await updatePoolRatingsInDatabase(pool.pool_id, rating, apy);
            return { pool_id: pool.pool_id, rating, apy };
        });

        await Promise.all(updatePromises);
        console.log('Pools ratings updated successfully in the database');
    } catch (error) {
        console.error('Error updating pool ratings:', error.message);
    }
}

// Directly call the function if this script is run from the command line
updateRatings();