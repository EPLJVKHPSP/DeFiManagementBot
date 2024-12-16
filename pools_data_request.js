const fs = require('fs');
const axios = require('axios');
const dbPool = require('./db'); // Database connection pool setup

const apiPoolsUrl = 'https://yields.llama.fi/pools';
const apiChartUrl = 'https://yields.llama.fi/chart/';
const jsonFilePath = 'data/pools.json'; // Local JSON file to save data

// Step 1: Fetch data from API and overwrite pools.json
async function fetchAndSavePoolsData() {
    try {
        console.log('Fetching latest pools data from API...');
        const response = await axios.get(apiPoolsUrl);
        const poolsData = response.data;

        console.log('Writing data to pools.json...');
        fs.writeFileSync(jsonFilePath, JSON.stringify(poolsData, null, 2), 'utf-8');
        console.log('pools.json updated successfully.');
    } catch (error) {
        console.error('Error fetching data from API:', error.message);
        throw error;
    }
}

// Step 2: Fetch IDs from the database
async function fetchPoolIdsFromDatabase() {
    const query = 'SELECT id FROM ratings.pools;';
    try {
        console.log('Fetching pool IDs from database...');
        const result = await dbPool.query(query);
        return result.rows.map(row => row.id);
    } catch (error) {
        console.error('Error fetching IDs from database:', error.message);
        throw error;
    }
}

// Step 3: Update tokens, chain, protocol, and ROI in the database
async function updatePoolsInDatabase(poolIds) {
    try {
        console.log('Reading pools.json...');
        const poolsData = JSON.parse(fs.readFileSync(jsonFilePath, 'utf-8')).data;

        console.log('Updating database for matching pool IDs...');
        for (const id of poolIds) {
            const pool = poolsData.find(p => p.pool === id);
            if (!pool) {
                console.log(`No matching pool found in JSON for ID: ${id}`);
                continue;
            }

            const { chain, project: protocol, symbol, apy } = pool;

            let token1, token2;
            if (symbol.includes('-')) {
                [token1, token2] = symbol.split('-');
            } else {
                token1 = token2 = symbol;
            }

            const updateQuery = `
                UPDATE ratings.pools
                SET chain = $1, protocol = $2, token1 = $3, token2 = $4, roi = $5
                WHERE id = $6;
            `;

            const updateValues = [chain, protocol, token1, token2, apy, id];

            try {
                const result = await dbPool.query(updateQuery, updateValues);
                if (result.rowCount > 0) {
                    console.log(`Successfully updated pool ID: ${id}`);
                } else {
                    console.log(`No rows updated for pool ID: ${id}`);
                }
            } catch (error) {
                console.error(`Error updating pool ID ${id}:`, error.message);
            }
        }
    } catch (error) {
        console.error('Error updating pools in database:', error.message);
        throw error;
    }
}

// Step 4: Fetch pool chart data and calculate ratings
async function fetchPoolChartData(poolId) {
    const url = `${apiChartUrl}${poolId}`;
    try {
        const response = await axios.get(url);
        const data = response.data.data;

        if (data && data.length > 0) {
            const lastDataPoint = data[data.length - 1];
            return {
                tvlUsd: lastDataPoint.tvlUsd || 0,
                apy: lastDataPoint.apy || 0
            };
        } else {
            console.warn(`No data available for pool ${poolId}.`);
            return null;
        }
    } catch (error) {
        console.error(`Error fetching chart data for pool ${poolId}:`, error.message);
        return null;
    }
}

async function updatePoolRatings(poolId, token1, token2, protocol, chain, tvlUsd, apy) {
    const query = `
        UPDATE ratings.pools
        SET token1 = $1, token2 = $2, protocol = $3, chain = $4, rating = $5, roi = $6
        WHERE id = $7;
    `;

    try {
        // Example calculation for rating based on TVL and APY
        const rating = Math.round(tvlUsd * apy);

        await dbPool.query(query, [
            token1,
            token2,
            protocol,
            chain,
            rating,
            apy,
            poolId
        ]);
        console.log(`Successfully updated pool rating for ID: ${poolId}`);
    } catch (error) {
        console.error(`Error updating pool rating for ID ${poolId}:`, error.message);
    }
}

async function calculateAndUpdateRatings() {
    const query = `SELECT id, token1, token2, protocol, chain FROM ratings.pools;`;
    try {
        const result = await dbPool.query(query);
        const updatePromises = result.rows.map(async (row) => {
            const { id, token1, token2, protocol, chain } = row;

            const poolData = await fetchPoolChartData(id);
            if (poolData) {
                const { tvlUsd, apy } = poolData;
                await updatePoolRatings(id, token1, token2, protocol, chain, tvlUsd, apy);
            }
        });

        await Promise.all(updatePromises);
        console.log('All pool ratings calculated and updated successfully.');
    } catch (error) {
        console.error('Error calculating or updating pool ratings:', error.message);
    }
}

// Orchestrating the entire process
async function main() {
    try {
        // Step 1: Fetch and save data to pools.json
        await fetchAndSavePoolsData();

        // Step 2: Fetch IDs and update tokens, protocol, and chain in database
        const poolIds = await fetchPoolIdsFromDatabase();
        await updatePoolsInDatabase(poolIds);

        // Step 3: Calculate and update pool ratings
        await calculateAndUpdateRatings();
    } catch (error) {
        console.error('Error during the process:', error.message);
    } finally {
        dbPool.end(() => {
            console.log('Database connection pool closed.');
        });
    }
}

// Execute the script
main();