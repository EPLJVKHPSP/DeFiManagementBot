const axios = require('axios');
const fs = require('fs');
const { parse } = require('csv-parse');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const poolFile = './data/pools.csv';  // Ensure the path is relative and correct

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
            const apy = lastDataPoint.apy || 'No APY Data';  // Default if APY is missing

            return {
                rating: (tvlUsd * diffDays).toString(),  // Calculate rating
                apy: apy.toString()  // Store APY value
            };
        }
        return { rating: 'No data available', apy: 'No APY Data' };  // Defaults if no data
    } catch (error) {
        console.error('Error fetching data for:', poolId, error);
        return {
            rating: 'Error fetching data',
            apy: 'Error'
        };
    }
}

async function updateRatings() {
    const inputData = fs.readFileSync(poolFile, { encoding: 'utf8' });
    parse(inputData, {
        columns: true,
        trim: true,
        skip_empty_lines: true,
        relax_column_count: true
    }, async (err, pools) => {
        if (err) {
            console.error('Error reading CSV file:', err);
            return;
        }

        console.log('CSV with Pools successfully processed');
        const updatePromises = pools.map(async (pool) => {
            const poolData = await fetchPoolData(pool.PoolID);
            pool.Rating = poolData.rating;
            pool.ROI = poolData.apy;  // Update APY to ROI column
            return pool;
        });

        const updatedPools = await Promise.all(updatePromises);
        updatedPools.sort((a, b) => parseFloat(b.Rating.replace(/[^0-9.-]+/g, "")) - parseFloat(a.Rating.replace(/[^0-9.-]+/g, "")));

        const csvWriter = createCsvWriter({
            path: poolFile,
            header: [
                { id: 'Token1', title: 'Token1' },
                { id: 'Token2', title: 'Token2' },
                { id: 'Protocol', title: 'Protocol' },
                { id: 'PoolID', title: 'PoolID' },
                { id: 'Rating', title: 'Rating' },
                { id: 'ROI', title: 'ROI' },
                { id: 'StrategyRating', title: 'StrategyRating' }
            ]
        });

        await csvWriter.writeRecords(updatedPools)
            .then(() => {
                console.log('Pools Ratings updated and written to file successfully');
            })
            .catch(error => {
                console.error('Failed to write CSV:', error);
            });
    });
}

// Directly calling the function if this script is run from the command line
updateRatings();