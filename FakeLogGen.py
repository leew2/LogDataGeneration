import random
import pandas as pd
from datetime import datetime, timedelta
import os
import logging

def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Parameters
    num_rows = 10000
    start_time = datetime(2025, 1, 1, 8, 0, 0)
    outcomes = ['success', 'failure', 'timeout', 'cancelled']
    priorities = ['low', 'medium', 'high', 'critical']


    data = []
    for i in range(num_rows):
        timestamp = start_time + timedelta(seconds=random.randint(0, 60*60*24*30))  # within 30 days
        duration = round(random.uniform(0.5, 120.0), 2)  # duration in seconds
        outcome = random.choice(outcomes)
        priority = random.choice(priorities)
        data.append([timestamp.strftime('%Y-%m-%d %H:%M:%S'), duration, outcome, priority])

    # Create DataFrame, sort by timestamp, and save to JSON
    df = pd.DataFrame(data, columns=['timestamp', 'duration', 'outcome', 'priority'])
    df = df.sort_values('timestamp')
    df = df.head(10000)  # Ensure only 10,000 rows are saved
    output_dir = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(output_dir, exist_ok=True)
    json_path = os.path.join(output_dir, 'synthetic_data.json')
    df.to_json(json_path, orient='records', lines=True)
    logging.info(f"Synthetic data saved to {json_path}")

if __name__ == '__main__':
    main()
