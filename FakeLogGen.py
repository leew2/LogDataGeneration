import random
import pandas as pd
from datetime import datetime, time, timedelta
import os
import logging
import json
import logging.config
import time
from prometheus_client import start_http_server, Counter, Gauge


# Main =======================================================================================================================



def main(log_path, run_counter=None):
    setup_logging(log_path)
    # Parameters
    num_rows = 10000
    start_time = datetime(2025, 1, 1, 8, 0, 0)
    outcomes = ['success', 'failure', 'timeout', 'cancelled']
    priorities = ['low', 'medium', 'high', 'critical']

    # Generate data with retry
    df = generate_data_with_retry(num_rows, start_time, outcomes, priorities)
    df = df.sort_values('timestamp')
    df = df.head(10000)  # Ensure only 10,000 rows are saved



    # Save data
    output_dir = os.path.join(os.path.dirname(__file__), 'data')
    json_path = save_dataframe_to_json(df, output_dir, 'synthetic_data.json')
    logging.info(f"Synthetic data saved to {json_path}")
    if run_counter is not None:
        run_counter.inc()

# ==================================================================================================================


def generate_synthetic_data(num_rows, start_time, outcomes, priorities, success_weight=0.7):
    """
    Generates synthetic data. Outcome is randomly chosen from the outcomes list for each row,
    but 'success' is more likely (default 70%) than the other outcomes.
    You can adjust success_weight (0.0-1.0) to control the chance of 'success'.
    """
    # Distribute remaining probability equally among other outcomes
    other_outcomes = [o for o in outcomes if o != 'success']
    if other_outcomes:
        other_weight = (1.0 - success_weight) / len(other_outcomes)
        weights = [success_weight if o == 'success' else other_weight for o in outcomes]
    else:
        weights = [1.0 for _ in outcomes]

    data = []
    for i in range(num_rows):
        timestamp = start_time + timedelta(seconds=random.randint(0, 60*60*24*30))  # within 30 days
        duration = round(random.uniform(0.5, 120.0), 2)  # duration in seconds
        outcome = random.choices(outcomes, weights=weights, k=1)[0]
        priority = random.choice(priorities)
        data.append([timestamp.strftime('%Y-%m-%d %H:%M:%S'), duration, outcome, priority])
    df = pd.DataFrame(data, columns=['timestamp', 'duration', 'outcome', 'priority'])
    return df

def save_dataframe_to_json(df, output_dir, filename):
    os.makedirs(output_dir, exist_ok=True)
    json_path = os.path.join(output_dir, filename)
    df.to_json(json_path, orient='records', lines=True)
    return json_path

def generate_data_with_retry(num_rows, start_time, outcomes, priorities, max_retries=3, delay=2):
    """
    Tries to generate synthetic data, retrying up to max_retries times if an exception occurs.
    Returns the DataFrame if successful, or raises the last exception.
    """
    for attempt in range(1, max_retries + 1):
        try:
            df = generate_synthetic_data(num_rows, start_time, outcomes, priorities)
            return df
        except Exception as e:
            logging.error(f"Data failed on attempt {attempt}: {e}")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                logging.critical("Max retries reached. Failed.")
                raise

def get_percentages(df):
        counts = df['outcome'].value_counts(normalize=True) * 100
        return counts.reindex(['failure', 'timeout', 'cancelled', 'success']).fillna(0)

def show_data_improvement(df_old, df_new):
    old_rows = len(df_old)
    new_rows = len(df_new)
    old_outcomes = set(df_old['outcome'])
    new_outcomes = set(df_new['outcome'])
    old_priorities = set(df_old['priority'])
    new_priorities = set(df_new['priority'])

    # Compare 'failure' counts
    old_failures = (df_old['outcome'] == 'failure').sum()
    new_failures = (df_new['outcome'] == 'failure').sum()

    # Compare duration statistics
    old_duration_sum = df_old['duration'].sum()
    new_duration_sum = df_new['duration'].sum()
    old_duration_mean = df_old['duration'].mean()
    new_duration_mean = df_new['duration'].mean()

    old_percent = get_percentages(df_old)
    new_percent = get_percentages(df_new)
    comparison = pd.DataFrame({'Old (%)': old_percent, 'New (%)': new_percent})
    logging.info("\nOutcome percentage comparison:\n" + str(comparison))

    logging.info(f"Old data rows: {old_rows}, New data rows: {new_rows}")
    logging.info(f"Old unique outcomes: {old_outcomes}, New unique outcomes: {new_outcomes}")
    logging.info(f"Old unique priorities: {old_priorities}, New unique priorities: {new_priorities}")
    logging.info(f"Old duration sum: {old_duration_sum:.2f}, New duration sum: {new_duration_sum:.2f}")
    logging.info(f"Old duration mean: {old_duration_mean:.2f}, New duration mean: {new_duration_mean:.2f}")

    # Compare outcome counts
    old_outcome_counts = df_old['outcome'].value_counts().to_dict()
    new_outcome_counts = df_new['outcome'].value_counts().to_dict()
    logging.info(f"Old outcome counts: {old_outcome_counts}")
    logging.info(f"New outcome counts: {new_outcome_counts}")

    for outcome in sorted(old_outcomes.union(new_outcomes)):
        old_count = old_outcome_counts.get(outcome, 0)
        new_count = new_outcome_counts.get(outcome, 0)
        if old_count != new_count:
            logging.info(f"Outcome '{outcome}': old count = {old_count}, new count = {new_count}, diff = {new_count - old_count}")
        else:
            logging.info(f"Outcome '{outcome}': count unchanged at {old_count}")

    if new_rows > old_rows:
        logging.info(f"Data improved: {new_rows - old_rows} more rows.")
    elif new_rows < old_rows:
        logging.info(f"Data reduced: {old_rows - new_rows} fewer rows.")
    else:
        logging.info("No change in row count.")

    if new_outcomes != old_outcomes:
        logging.info("Outcome categories have changed.")
    if new_priorities != old_priorities:
        logging.info("Priority categories have changed.")

    if new_failures > old_failures:
        logging.info(f"'failure' count increased by {new_failures - old_failures}.")
    elif new_failures < old_failures:
        logging.info(f"'failure' count decreased by {old_failures - new_failures}.")
    else:
        logging.info("No change in 'failure' count.")


# Example usage:
def stimulate_difference():
    # Old data starts earlier and covers a longer period, new data starts later and covers a shorter period
    start_time = datetime(2025, 1, 1, 8, 0, 0)
    outcomes = ['success', 'failure', 'timeout', 'cancelled']
    priorities = ['low', 'medium', 'high', 'critical']

    # Randomize the amount of data generated
    old_num_rows = random.randint(5000, 10000)
    new_num_rows = random.randint(4000, 7000)

    # Generate old data (default success chance)
    df_old = generate_synthetic_data(old_num_rows, start_time, outcomes, priorities, success_weight=0.5)
    # Generate new data (higher success chance)
    df_new = generate_synthetic_data(new_num_rows, start_time, outcomes, priorities, success_weight=0.80)

    show_data_improvement(df_old, df_new)



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

def setup_logging(log_path):
    log_dir = os.path.dirname(log_path)
    os.makedirs(log_dir, exist_ok=True)
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

def clear_log_file(log_path):
    if os.path.exists(log_path):
        with open(log_path, 'w') as f:
            f.truncate(0)
        print(f"Cleared log file: {log_path}")
    else:
        print(f"Log file does not exist, nothing to clear: {log_path}")


# ==================================================================================================================




if __name__ == '__main__':
    log_path = os.path.join(os.path.dirname(__file__), 'data', 'FakeLogGen.log')
    clear_log_file(log_path)
    # Prometheus metrics
    run_counter = Counter('fake_loggen_runs_total', 'Total number of times main() has run')
    log_lines_gauge = Gauge('fake_loggen_log_lines', 'Number of log lines in FakeLogGen.log')
    start_http_server(8000)

    counter = 0
    while True:
        main(log_path, run_counter)
        if os.path.exists(log_path):
            with open(log_path, 'r', encoding='utf-8') as f:
                log_lines = sum(1 for _ in f)
            log_lines_gauge.set(log_lines)
        else:
            log_lines_gauge.set(0)
        counter += 1
        logging.info(f"Run count: {counter}")
        if counter != 0 and counter % 5 == 0:
            user_input = input("Counter is a multiple of 5. Do you want to stop? (y/n): ").strip().lower()
            if user_input == 'y':
                logging.info("User chose to stop the loop.")
                break
            elif user_input != 'n':
                logging.warning("Invalid input. Continuing the loop.")
        time.sleep(1)
    stimulate_difference()


