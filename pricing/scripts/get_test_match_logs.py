"""
This script creates the test set with relevant match logs.
The test set consists of post-transfer matches for players transferred to the top 5 european leagues in the last
4 seasons.
To be able to perform feature engineering down the line, we also include the match logs for these players
before they moved to their new clubs.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import fire
import pandas as pd

from pricing.format.match_logs import (
    find_latest_post_transfer_file,
    load_post_transfer_match_logs,
)

DATA_PATH = Path(__file__).parent.parent.parent / "data"
PROCESSED_DATA_PATH = DATA_PATH / "processed"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def find_latest_transferred_player_match_logs_file(processed_data_path: Path) -> Optional[Path]:
    """Find the most recent transferred player match logs file"""
    files = list(processed_data_path.glob("transferred_player_match_logs_*.csv"))
    if not files:
        return None
    return max(files, key=lambda x: x.stat().st_mtime)


def load_transferred_player_match_logs(file_path: str) -> pd.DataFrame:
    """
    Load transferred player match logs
    """
    df = pd.read_csv(file_path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def build_combined_transfer_dataset(df_post_transfer: pd.DataFrame, df_all_matches: pd.DataFrame) -> pd.DataFrame:
    """
    Build a dataset combining post-transfer matches with pre-transfer matches for transferred players.
    Handles multiple transfers for the same player.

    Args:
        df_post_transfer: DataFrame containing post-transfer match logs
        df_all_matches: DataFrame containing match logs for all players around transfers

    Returns:
        DataFrame with pre- and post-transfer match logs tagged appropriately
    """
    logging.info("Building combined transfer dataset...")

    # Ensure date columns are datetime
    df_post_transfer["date"] = pd.to_datetime(df_post_transfer["date"])
    df_all_matches["date"] = pd.to_datetime(df_all_matches["date"])

    combined_logs = []
    unique_transfer_ids = df_post_transfer["transfer_id"].unique()

    for transfer_id in unique_transfer_ids:
        # Get post-transfer data for this specific transfer
        transfer_data = df_post_transfer.loc[df_post_transfer["transfer_id"] == transfer_id, :].copy()

        if transfer_data.empty:
            continue

        # Extract transfer info
        player_id = transfer_data["player_id"].iloc[0]
        from_club = transfer_data["from_club"].iloc[0]
        to_club = transfer_data["to_club"].iloc[0]
        transfer_date = transfer_data["transfer_date"].iloc[0]

        # Flag post-transfer data
        transfer_data["is_post_transfer"] = True
        transfer_data["is_pre_transfer"] = False

        # Find pre-transfer data in all matches
        player_matches = df_all_matches[
            (df_all_matches["player_id"] == player_id)
            & (df_all_matches["team"] == from_club)
            & (df_all_matches["date"] < transfer_date)
        ].copy()

        if not player_matches.empty:
            post_transfer_seasons = transfer_data["season"].unique()

            # Get earliest post-transfer season
            earliest_post_season = min(post_transfer_seasons) if len(post_transfer_seasons) > 0 else None

            if earliest_post_season and "season" in player_matches.columns:
                if "-" in earliest_post_season:
                    post_season_start_year = int(earliest_post_season.split("-")[0])

                    # Include matches from the same season (mid-season transfers)
                    # and previous season (between-season transfers)
                    same_season_matches = player_matches[player_matches["season"] == earliest_post_season]

                    previous_season = f"{post_season_start_year-1}-{post_season_start_year}"
                    previous_season_matches = player_matches[player_matches["season"] == previous_season]

                    pre_transfer_data = pd.concat([same_season_matches, previous_season_matches])
                else:
                    # If season format is different, fall back to time-based approach
                    cutoff_date = transfer_date - pd.Timedelta(days=365)
                    pre_transfer_data = player_matches[player_matches["date"] >= cutoff_date]
            else:
                # Fall back to time-based approach if season info is not available
                cutoff_date = transfer_date - pd.Timedelta(days=365)
                pre_transfer_data = player_matches[player_matches["date"] >= cutoff_date]

            if not pre_transfer_data.empty:
                # Sort pre-transfer data by date
                pre_transfer_data = pre_transfer_data.sort_values("date", ascending=True)

                # Add transfer information to pre-transfer data
                pre_transfer_data["transfer_id"] = transfer_id
                pre_transfer_data["transfer_date"] = transfer_date
                pre_transfer_data["from_club"] = from_club
                pre_transfer_data["to_club"] = to_club
                pre_transfer_data["is_post_transfer"] = False
                pre_transfer_data["is_pre_transfer"] = True
                pre_transfer_data["days_since_transfer"] = (pre_transfer_data["date"] - transfer_date).dt.days

                # Add match_number_after_transfer for pre-transfer games (negative numbers counting backward)
                total_pre_matches = len(pre_transfer_data)
                pre_transfer_data["match_number_after_transfer"] = range(-total_pre_matches, 0)

                # Combine with post-transfer data
                combined_logs.append(pd.concat([pre_transfer_data, transfer_data]))
            else:
                combined_logs.append(transfer_data)
        else:
            combined_logs.append(transfer_data)

    if not combined_logs:
        logging.warning("No combined logs found!")
        return pd.DataFrame()

    combined_df = pd.concat(combined_logs, ignore_index=True)

    # Remove any duplicate matches (post-transfer data takes precedence)
    combined_df.sort_values(["is_post_transfer", "date"], ascending=[False, True], inplace=True)
    combined_df.drop_duplicates(subset=["player_id", "date", "team", "opponent"], keep="first", inplace=True)

    logging.info(f"Created combined dataset with {len(combined_df)} match logs")
    return combined_df


def create_test_match_logs(post_transfer_file: str = None, transferred_player_file: str = None) -> None:
    """
    Create test match logs dataset by combining post-transfer match logs with
    pre-transfer match logs for the same players.

    Args:
        post_transfer_file: CSV file with post-transfer match logs (if None, uses latest)
        transferred_player_file: CSV file with all transferred player match logs (if None, uses latest)
    """
    logging.info("Starting test match logs creation process")

    # Find latest post-transfer match logs file if not specified
    if not post_transfer_file:
        latest_file = find_latest_post_transfer_file(PROCESSED_DATA_PATH)
        if latest_file is None:
            logging.error("No post-transfer match logs files found!")
            return
        post_transfer_file = str(latest_file)

    # Find latest transferred player match logs file if not specified
    if not transferred_player_file:
        latest_file = find_latest_transferred_player_match_logs_file(PROCESSED_DATA_PATH)
        if latest_file is None:
            logging.error("No transferred player match logs files found!")
            return
        transferred_player_file = str(latest_file)

    logging.info(f"Loading post-transfer match logs from {post_transfer_file}")
    df_post_transfer = load_post_transfer_match_logs(post_transfer_file)
    logging.info(f"Loaded {len(df_post_transfer)} post-transfer match logs")

    logging.info(f"Loading transferred player match logs from {transferred_player_file}")
    df_transferred_players = load_transferred_player_match_logs(transferred_player_file)
    logging.info(f"Loaded {len(df_transferred_players)} transferred player match logs")

    # Build combined dataset
    df_combined = build_combined_transfer_dataset(df_post_transfer, df_transferred_players)

    if df_combined.empty:
        logging.warning("No combined match logs found!")
        return

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d")
    output_path = PROCESSED_DATA_PATH / f"test_match_logs_{timestamp}.csv"
    logging.info(f"Saving test match logs to {output_path}")

    df_combined.to_csv(output_path, index=False)
    logging.info("Test match logs creation completed successfully")


if __name__ == "__main__":
    fire.Fire(create_test_match_logs)
