const fs = require('fs');
const { parse } = require('csv-parse/sync');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const tokenFile = './data/tokens.csv';
const poolFile = './data/pools.csv';

// Load token ratings from CSV file
function loadTokenRatings() {
    const tokensData = fs.readFileSync(tokenFile, { encoding: 'utf8' });
    const tokens = parse(tokensData, { columns: true, skip_empty_lines: true });
    const tokenRatings = {};
    tokens.forEach(token => {
        tokenRatings[token['Token-Name']] = parseFloat(token['Tier']);
    });
    return tokenRatings;
}

// Calculate and update strategy ratings
async function calculateStrategyRatings() {
    const tokenRatings = loadTokenRatings();
    const poolData = fs.readFileSync(poolFile, { encoding: 'utf8' });
    const pools = parse(poolData, { columns: true, skip_empty_lines: true });

    pools.forEach(pool => {
        const token1Rating = tokenRatings[pool.Token1] || 0;
        const token2Rating = tokenRatings[pool.Token2] || 0;
        const averageTokenRating = (token1Rating + token2Rating) / 2;

        if (averageTokenRating > 0) {
            pool.StrategyRating = pool.Rating / averageTokenRating / 10000000;
        } else {
            pool.StrategyRating = ''; // No rating if no valid token ratings
        }
    });

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

    await csvWriter.writeRecords(pools)
        .then(() => {
            console.log('Strategy Ratings updated and written to file successfully');
        })
        .catch(error => {
            console.error('Failed to write CSV:', error);
        });
}

calculateStrategyRatings().catch(console.error);
