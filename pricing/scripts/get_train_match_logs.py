"""
This script gets the match logs for the training data.
The training data consists of non post-transfer matches for players in the top 5 european leagues.
You need to run the get_post_transfer_match_logs.py script first to get the post-transfer match logs.
"""

import logging
from datetime import datetime
from pathlib import Path

import fire
import pandas as pd
from tqdm import tqdm

from pricing.format.match_logs import (
    create_match_id,
    create_player_match_id,
    find_latest_post_transfer_file,
    load_elo_data,
    load_match_logs,
    load_post_transfer_match_logs,
    merge_elo_data,
)

DATA_PATH = Path(__file__).parent.parent.parent / "data"
FBREF_MATCH_LOGS_PATH = DATA_PATH / "fbref" / "match_logs"
FBREF_ELO_PATH = DATA_PATH / "fbref" / "elo"
PROCESSED_DATA_PATH = DATA_PATH / "processed"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def tag_same_season_transfer_matches(df_training: pd.DataFrame) -> pd.DataFrame:
    """
    Tag all matches for a player in a club for a season if any of the matches
    in that season for the player in that club are post-transfer matches.
    """
    df = df_training.copy()

    # Extract season start year from season column (e.g. 2021-2022 -> 2021)
    df["season_start_year"] = df["season"].apply(lambda x: int(x[:4]))

    # Group by player_id, team and season
    grouped = df.groupby(["player_id", "team", "season_start_year"])

    # Initialize the new column
    df["is_transfer_season"] = False

    # For each group, check if any match is a post-transfer match
    for (player_id, team, season), group in tqdm(grouped):
        if group["is_post_transfer_match"].any():
            # If yes, tag all matches in that group
            mask = (df["player_id"] == player_id) & (df["team"] == team) & (df["season_start_year"] == season)
            df.loc[mask, "is_transfer_season"] = True

    logging.info(f"Tagged {df['is_transfer_season'].sum()} matches as being in transfer seasons")

    return df


def get_match_logs(
    match_logs_pattern: str = "*.csv", post_transfer_file: str = None, elo_pattern: str = "*.csv", add_elo: bool = False
) -> None:
    """
    Process match logs to extract training data (non post-transfer matches) with ELO ratings

    Args:
        match_logs_pattern: Pattern to match match log files
        post_transfer_file: CSV file with post-transfer matches (if None, uses latest)
        elo_pattern: Pattern to match ELO data files
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
    if not post_transfer_file:
        latest_file = find_latest_post_transfer_file(PROCESSED_DATA_PATH)
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

    # Tag post-transfer matches
    df_training = df_match_logs.copy()
    df_training["is_post_transfer_match"] = df_training["player_match_id"].isin(post_transfer_ids)
    logging.info(f"Created training set with {len(df_training)} player-match combinations")

    df_training = tag_same_season_transfer_matches(df_training)

    if add_elo:
        # Load and merge ELO data
        logging.info("Loading ELO data")
        all_elo_data = []
        for elo_file in FBREF_ELO_PATH.glob(elo_pattern):
            logging.info(f"Processing ELO file {elo_file.name}")
            df_elo = load_elo_data(str(elo_file))
            all_elo_data.append(df_elo)

        if not all_elo_data:
            logging.warning("No ELO data files found! Training data will not include ELO information.")
        else:
            df_elo_all = pd.concat(all_elo_data, ignore_index=True)
            logging.info(f"Loaded {len(df_elo_all)} ELO data entries")

            # Merge ELO data with training data
            logging.info("Merging ELO data with training data")
            df_training = merge_elo_data(df_training, df_elo_all)
            logging.info("Added ELO metrics to training data")

    # Save training data
    timestamp = datetime.now().strftime("%Y%m%d")
    output_path = PROCESSED_DATA_PATH / f"training_match_logs_{timestamp}.csv"
    logging.info(f"Saving training match logs to {output_path}")

    df_training.to_csv(output_path, index=False)
    logging.info("Training match logs processing completed successfully")


if __name__ == "__main__":
    fire.Fire(get_match_logs)
