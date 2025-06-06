"""
This script gets the FBRef match logs for the post-transfer matches of the players in the transfers file
with relevant transfers
"""

import logging
from datetime import datetime
from pathlib import Path

import fire
import pandas as pd

from pricing.format.match_logs import get_post_transfer_match_logs, load_match_logs
from pricing.format.transfer import find_latest_transfers_mapped_file, load_transfers_mapped_names

DATA_PATH = Path(__file__).parent.parent.parent / "data"
FBREF_MATCH_LOGS_PATH = DATA_PATH / "fbref" / "match_logs"
PROCESSED_DATA_PATH = DATA_PATH / "processed" / "final"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_match_logs(
    match_logs_pattern: str = "*.csv", transfers_file: str = None, number_of_post_transfer_matches: int = 10
) -> None:
    """
    Process match logs to extract post-transfer performance data

    Args:
        match_logs_pattern: Pattern to match match log files
        transfers_file: CSV file with processed transfers (if None, uses latest)
        number_of_post_transfer_matches: Number of matches to analyze after transfer
    """
    logging.info("Starting post-transfer match logs processing")

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
    logging.info(f"Loaded {len(df_match_logs)} match log entries")

    # Find latest transfers file if not specified
    if not transfers_file:
        latest_file = find_latest_transfers_mapped_file(PROCESSED_DATA_PATH)
        if latest_file is None:
            logging.error("No processed transfer files found!")
            return
        transfers_file = str(latest_file)

    logging.info(f"Loading transfers from {transfers_file}")
    df_transfers = load_transfers_mapped_names(transfers_file)
    logging.info(f"Loaded {len(df_transfers)} transfers")

    # Process post-transfer match logs
    logging.info("Getting post-transfer match logs...")
    df_post_transfer = get_post_transfer_match_logs(
        df_match_logs=df_match_logs,
        transfers_mapped_names=df_transfers,
        number_of_post_transfer_matches=number_of_post_transfer_matches,
    )

    if df_post_transfer is None or len(df_post_transfer) == 0:
        logging.warning("No post-transfer match logs found!")
        return

    logging.info(f"Found {len(df_post_transfer)} post-transfer match logs")

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d")
    output_path = PROCESSED_DATA_PATH / f"post_transfer_match_logs_{timestamp}.csv"
    logging.info(f"Saving post-transfer match logs to {output_path}")

    df_post_transfer.to_csv(output_path, index=False)
    logging.info("Post-transfer match logs processing completed successfully")


if __name__ == "__main__":
    fire.Fire(get_match_logs)
