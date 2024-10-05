#allocation.py
import os
import sys
import logging
import numpy as np
import pandas as pd

from scipy.optimize import minimize

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

allocation = 10000000

def load_data(filepath):
    """Load data from CSV."""
    try:
        data = pd.read_csv(filepath)
        logging.info(f"Loaded data from {filepath}")
        return data
    except Exception as e:
        logging.error(f"Error loading data from {filepath}: {e}")
        raise

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
    result = minimize(variance_risk_contributions, initial_weights, args=(Sigma_norm,),
                      method='SLSQP', bounds=bounds, constraints=constraints)
    optimal_weights = result.x
    return optimal_weights * total_allocation

def variance_risk_contributions(weights, Sigma):
    """Calculate the variance of risk contributions."""
    contributions = risk_contributions(weights, Sigma)
    return np.var(contributions)

def risk_contributions(weights, Sigma):
    """Calculate risk contributions for given weights."""
    total_risk = np.dot(weights, np.dot(Sigma, weights))
    marginal_contributions = np.dot(Sigma, weights)
    return weights * marginal_contributions / total_risk

def save_allocation_summary_to_csv(summary, output_filepath):
    """Save the allocation summary to a CSV file formatted to two decimal places."""
    summary['Allocation ($)'] = summary['Allocation ($)'].apply(lambda x: f"{x:.2f}")
    summary['Weight (%)'] = summary['Weight (%)'].apply(lambda x: f"{x:.2f}")

    summary.to_csv(output_filepath, index=False)
    logging.info(f"Allocation summary saved to {output_filepath}")

def main():

    filepath = './data/pools.csv'
    protocols_filepath = './data/protocols.csv'
    output_filepath = 'allocation_summary.csv'
    total_allocation = allocation

    try:
        protocols_df = load_data(filepath)
        protocols_df['Strategy'] = protocols_df.apply(
            lambda row: f"{row['Token1'].split('(')[0]} ({row['Token1'].split('(')[1][:-1]})" if row['Token1'] == row['Token2']
            else f"{row['Token1'].split('(')[0]}/{row['Token2'].split('(')[0]} ({row['Token1'].split('(')[1][:-1]})",
            axis=1
        )

        tier_df = load_data(protocols_filepath)
        protocols_df = protocols_df.merge(tier_df, on='Protocol', how='left')

        # Allocation according to the Tiers of Protocol
        # Rules are in allocation_rules.xlsx

        tier_allocations = {
            1: 0.5 * total_allocation,
            2: 0.3 * total_allocation,
            3: 0.15 * total_allocation,
            4: 0.05 * total_allocation
        }

        all_results = []

        for tier, group in protocols_df.groupby('Tier'):

            n = len(group)

            initial_weights = np.ones(n) / n

            constraints, bounds = define_constraints(n)

            inverted_risk_scores = calculate_inverted_risk_scores(group['StrategyRating'].values)

            Sigma = create_risk_matrix(inverted_risk_scores)

            Sigma_norm = normalize_risk_matrix(Sigma)

            optimal_weights = optimize_weights(Sigma_norm, initial_weights, tier_allocations[tier], constraints, bounds)

            group['Allocation ($)'] = optimal_weights
            group['Weight (%)'] = 100 * optimal_weights / total_allocation
            
            all_results.append(group[['Strategy', 'Protocol', 'ROI', 'Allocation ($)', 'Weight (%)']])

        final_summary = pd.concat(all_results).sort_values(by='Weight (%)', ascending=False)

        final_summary['ROI'] = pd.to_numeric(final_summary['ROI'])

        # Converting Weight (%) to numeric
        final_summary['Weight (%)'] = pd.to_numeric(final_summary['Weight (%)'])

        # Calculating the weighted average ROI
        weighted_roi = (final_summary['ROI'] * final_summary['Weight (%)']).sum() / 100  # Calculate weighted average ROI
        logging.info(f"Weighted Average ROI: {weighted_roi:.2f}")

        # Saving the summary to CSV
        save_allocation_summary_to_csv(final_summary, output_filepath)

    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == '__main__':
    main()