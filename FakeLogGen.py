import random
import pandas as pd
from datetime import datetime, time, timedelta
import os
import logging
import json
import logging.config
import time


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



# Custom JSON formatter for logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'time': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'name': record.name,
            'message': record.getMessage(),
        }
        return json.dumps(log_record)

def setup_logging():
    log_dir = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, 'FakeLogGen.log')
    LOGGING_CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'json': {
                '()': JsonFormatter,
            },
        },
        'handlers': {
            'rotating_file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': log_path,
                'maxBytes': 1024*1024,  # 1MB
                'backupCount': 3,
                'formatter': 'json',
                'level': 'INFO',
                'encoding': 'utf-8',
            },
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'json',
                'level': 'INFO',
            },
        },
        'root': {
            'handlers': ['rotating_file', 'console'],
            'level': 'INFO',
        },
    }
    logging.config.dictConfig(LOGGING_CONFIG)
# ==================================================================================================================

if __name__ == '__main__':

    counter = 0
    while True:
        main()
        counter += 1
        logging.info(f"Run count: {counter}")
        if counter != 0 and counter % 5 == 0:
            user_input = input("Counter is a multiple of 5. Do you want to stop? (y/n): ").strip().lower()
            if user_input == 'y':
                logging.info("User chose to stop the loop.")
                break
            elif user_input != 'n':
                logging.warning("Invalid input. Continuing the loop.")
        time.sleep(2)



