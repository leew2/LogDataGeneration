import random
import pandas as pd
from datetime import datetime, timedelta
import os
import logging

# Main =======================================================================================================================

def main():
    setup_logging()
    # Parameters
    num_rows = 10000
    start_time = datetime(2025, 1, 1, 8, 0, 0)
    outcomes = ['success', 'failure', 'timeout', 'cancelled']
    priorities = ['low', 'medium', 'high', 'critical']

    # Generate data
    df = generate_synthetic_data(num_rows, start_time, outcomes, priorities)
    df = df.sort_values('timestamp')
    df = df.head(10000)  # Ensure only 10,000 rows are saved

    # Save data
    output_dir = os.path.join(os.path.dirname(__file__), 'data')
    json_path = save_dataframe_to_json(df, output_dir, 'synthetic_data.json')
    logging.info(f"Synthetic data saved to {json_path}")

# ==================================================================================================================

def generate_synthetic_data(num_rows, start_time, outcomes, priorities):
    data = []
    for i in range(num_rows):
        timestamp = start_time + timedelta(seconds=random.randint(0, 60*60*24*30))  # within 30 days
        duration = round(random.uniform(0.5, 120.0), 2)  # duration in seconds
        outcome = random.choice(outcomes)
        priority = random.choice(priorities)
        data.append([timestamp.strftime('%Y-%m-%d %H:%M:%S'), duration, outcome, priority])
    df = pd.DataFrame(data, columns=['timestamp', 'duration', 'outcome', 'priority'])
    return df

def save_dataframe_to_json(df, output_dir, filename):
    os.makedirs(output_dir, exist_ok=True)
    json_path = os.path.join(output_dir, filename)
    df.to_json(json_path, orient='records', lines=True)
    return json_path

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==================================================================================================================

if __name__ == '__main__':
    main()
