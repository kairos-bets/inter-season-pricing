"""
This script gets the match logs for the training data.
The training data consists of non post-transfer matches for players in the top 5 european leagues.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import fire
import pandas as pd

from pricing.format.match_logs import (
    create_match_id,
    create_player_match_id,
    load_match_logs,
    load_post_transfer_match_logs,
)

DATA_PATH = Path(__file__).parent.parent.parent / "data"
FBREF_MATCH_LOGS_PATH = DATA_PATH / "fbref" / "match_logs"
PROCESSED_DATA_PATH = DATA_PATH / "processed"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def find_latest_post_transfer_file() -> Optional[Path]:
    """Find the most recent post-transfer match logs file"""
    post_transfer_files = list(PROCESSED_DATA_PATH.glob("post_transfer_match_logs_*.csv"))
    if not post_transfer_files:
        return None
    return max(post_transfer_files, key=lambda x: x.stat().st_mtime)


def main(match_logs_pattern: str = "*.csv", post_transfer_file: str = None) -> None:
    """
    Process match logs to extract training data (non post-transfer matches)

    Args:
        match_logs_pattern: Pattern to match match log files
        post_transfer_file: CSV file with post-transfer matches (if None, uses latest)
    """
    logging.info("Starting training match logs processing")

    # Load all match logs
    logging.info(f"Loading match logs from {FBREF_MATCH_LOGS_PATH}")
    all_match_logs = []
    for match_log_file in FBREF_MATCH_LOGS_PATH.glob(match_logs_pattern):
        logging.info(f"Processing {match_log_file.name}")
        df_match_log = load_match_logs(match_log_file)
        all_match_logs.append(df_match_log)

    if not all_match_logs:
        logging.error("No match log files found!")
        return

    df_match_logs = pd.concat(all_match_logs, ignore_index=True)
    logging.info(f"Loaded {len(df_match_logs)} total match log entries")

    # Find latest post-transfer file if not specified
    if post_transfer_file is None:
        latest_file = find_latest_post_transfer_file()
        if latest_file is None:
            logging.error("No post-transfer match logs found!")
            return
        post_transfer_file = str(latest_file)

    # Load post-transfer match logs
    logging.info(f"Loading post-transfer match logs from {post_transfer_file}")
    df_post_transfer = load_post_transfer_match_logs(post_transfer_file)
    logging.info(f"Loaded {len(df_post_transfer)} post-transfer match logs")

    if df_post_transfer.empty:
        logging.error("Failed to load post-transfer match logs or file is empty")
        return

    # Create unique identifiers for matches
    logging.info("Creating unique match identifiers")
    df_match_logs = create_match_id(df_match_logs, "team", "opponent", "date")
    df_post_transfer = create_match_id(df_post_transfer, "team", "opponent", "date")
    df_match_logs = create_player_match_id(df_match_logs, "player_id", "match_id")
    df_post_transfer = create_player_match_id(df_post_transfer, "player_id", "match_id")

    # Extract post-transfer match IDs
    post_transfer_ids = set(df_post_transfer["player_match_id"])
    logging.info(f"Found {len(post_transfer_ids)} unique post-transfer player-match combinations")

    # Filter out post-transfer matches to create training set
    df_training = df_match_logs.loc[~df_match_logs["player_match_id"].isin(post_transfer_ids), :]
    logging.info(f"Created training set with {len(df_training)} player-match combinations")

    # Save training data
    timestamp = datetime.now().strftime("%Y%m%d")
    output_path = PROCESSED_DATA_PATH / f"training_match_logs_{timestamp}.csv"
    logging.info(f"Saving training match logs to {output_path}")

    df_training.to_csv(output_path, index=False)
    logging.info("Training match logs processing completed successfully")


if __name__ == "__main__":
    fire.Fire(main)
