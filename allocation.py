import logging
import numpy as np
import pandas as pd
from db_util import fetch_query, get_connection

def load_data_from_db(query, params=None):
    """Executes a SQL query and returns the result as a Pandas DataFrame."""
    try:
        logging.info(f"Executing query: {query}")
        result = fetch_query(query, params)
        if not result:
            logging.warning("Query returned no results.")
            return pd.DataFrame()
        return pd.DataFrame(result)
    except Exception as e:
        logging.error(f"Failed to load data from database: {e}")
        return pd.DataFrame()


def calculate_inverted_risk_scores(ratings):
    """Calculate inverted risk scores from ratings."""
    return 1 / ratings


def create_risk_matrix(inverted_risk_scores):
    """Create diagonal risk matrix from inverted risk scores."""
    return np.diag(inverted_risk_scores)


def normalize_risk_matrix(Sigma):
    """Normalize the risk matrix."""
    return Sigma / np.linalg.norm(Sigma)


def define_constraints(n):
    """Define optimization constraints and bounds."""
    constraints = ({'type': 'eq', 'fun': lambda w: np.sum(w) - 1})
    bounds = [(0, 1) for _ in range(n)]
    return constraints, bounds


def optimize_weights(Sigma_norm, initial_weights, total_allocation, constraints, bounds):
    """Optimize weights to minimize variance of risk contributions."""
    from scipy.optimize import minimize

    def variance_risk_contributions(weights, Sigma):
        total_risk = np.dot(weights, np.dot(Sigma, weights))
        marginal_contributions = np.dot(Sigma, weights)
        return np.var(weights * marginal_contributions / total_risk)

    result = minimize(
        variance_risk_contributions,
        initial_weights,
        args=(Sigma_norm,),
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )
    if not result.success:
        logging.warning(f"Optimization did not converge: {result.message}. Using initial weights.")
        return initial_weights * total_allocation
    return result.x * total_allocation


def redistribute_tiers(tier_allocations, ratings_df, global_limit, max_tier_limits):
    """Redistribute tier allocations dynamically based on available tiers."""
    available_tiers = ratings_df['tier'].unique()
    missing_tiers = {tier for tier in tier_allocations if tier not in available_tiers}
    redistribute_percentage = sum(tier_allocations[tier] for tier in missing_tiers)
    
    total_ratings = ratings_df.groupby('tier')['strategy_rating'].sum()
    total_ratings_all_tiers = total_ratings.sum()

    if total_ratings_all_tiers > 0:
        for tier in available_tiers:
            tier_allocations[tier] += (
                redistribute_percentage * (total_ratings[tier] / total_ratings_all_tiers)
            )
            tier_allocations[tier] = min(tier_allocations[tier], global_limit * max_tier_limits[tier])
    
    return tier_allocations


def save_allocation_to_db(summary):
    """Save the allocation summary into the database."""
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE ratings.allocation_summary;")
            for _, row in summary.iterrows():
                cursor.execute(
                    """
                    INSERT INTO ratings.allocation_summary (strategy, protocol, roi, allocation, weight)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (row['Strategy'], row['protocol_name'], row['roi'], row['Allocation ($)'], row['Weight (%)']),
                )
            conn.commit()
        logging.info("Allocation summary saved to database.")
    except Exception as e:
        logging.error(f"Error saving allocation summary to database: {e}")
        raise


def main():
    pools_query = "SELECT * FROM ratings.pools;"
    protocols_query = "SELECT protocol_name, tier FROM ratings.protocols;"
    output_filepath = 'allocation_summary.csv'
    global_limit = 200000  # Global investment limit

    try:
        logging.info("Loading data from database...")
        protocols_df = load_data_from_db(pools_query)
        tier_df = load_data_from_db(protocols_query)

        # Log column names for debugging
        logging.info(f"Columns in protocols_df: {protocols_df.columns}")
        logging.info(f"Columns in tier_df: {tier_df.columns}")

        if protocols_df.empty or tier_df.empty:
            logging.error("One or both DataFrames are empty. Exiting.")
            return

        # Normalize column names
        protocols_df.columns = protocols_df.columns.str.strip().str.lower()
        tier_df.columns = tier_df.columns.str.strip().str.lower()

        # Rename columns to ensure compatibility
        if 'protocol' in protocols_df.columns:
            logging.info("Renaming 'protocol' to 'protocol_name' in protocols_df")
            protocols_df.rename(columns={'protocol': 'protocol_name'}, inplace=True)

        if 'protocol' in tier_df.columns:
            logging.info("Renaming 'protocol' to 'protocol_name' in tier_df")
            tier_df.rename(columns={'protocol': 'protocol_name'}, inplace=True)

        # Merge data
        protocols_df = protocols_df.merge(tier_df, on='protocol_name', how='left')

        protocols_df['tier'] = pd.to_numeric(protocols_df['tier'], errors='coerce').fillna(4)
        protocols_df['strategy_rating'] = pd.to_numeric(protocols_df['strategy_rating'], errors='coerce').fillna(1)

        protocols_df['Strategy'] = protocols_df.apply(
            lambda row: f"{row['token1']}/{row['token2']} ({row['chain']})"
            if row['token1'] != row['token2'] else f"{row['token1']} ({row['chain']})", axis=1)

        # Allocation logic (unchanged)
        tier_allocations = {
            1: 0.5 * global_limit,
            2: 0.3 * global_limit,
            3: 0.15 * global_limit,
            4: 0.05 * global_limit
        }
        max_tier_limits = {1: 1.0, 2: 0.75, 3: 0.3, 4: 0.1}
        max_pool_limits = {1: 0.8, 2: 0.3, 3: 0.15, 4: 0.05}

        tier_allocations = redistribute_tiers(tier_allocations, protocols_df, global_limit, max_tier_limits)
        all_results = []

        for tier, group in protocols_df.groupby('tier'):
            if group.empty:
                continue

            initial_weights = np.ones(len(group)) / len(group)
            constraints, bounds = define_constraints(len(group))

            inverted_risk_scores = 1 / group['strategy_rating'].values
            Sigma = create_risk_matrix(inverted_risk_scores)
            Sigma_norm = normalize_risk_matrix(Sigma)

            tier_allocation_limit = tier_allocations[tier]
            optimal_weights = optimize_weights(
                Sigma_norm, initial_weights, tier_allocation_limit, constraints, bounds
            )

            group['Allocation ($)'] = np.minimum(optimal_weights, max_pool_limits[tier] * global_limit)
            group['Weight (%)'] = 100 * group['Allocation ($)'] / global_limit
            all_results.append(group[['Strategy', 'protocol_name', 'roi', 'Allocation ($)', 'Weight (%)']])

        if all_results:
            final_summary = pd.concat(all_results).sort_values(by='Weight (%)', ascending=False)
            save_allocation_to_db(final_summary)
            final_summary.to_csv(output_filepath, index=False)
            logging.info(f"Allocation summary saved to {output_filepath}")
        else:
            logging.warning("No results to save.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()