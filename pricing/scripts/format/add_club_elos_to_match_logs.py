"""
Add club ELO ratings to match logs data
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import fire
import pandas as pd
from tqdm import tqdm

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Path constants
DATA_PATH = Path(__file__).parent.parent.parent.parent / "data"
PROCESSED_DATA_PATH = DATA_PATH / "processed" / "final"


def find_latest_combined_elos_file() -> Path:
    """Find the most recent combined club ELOs file."""
    files = list(PROCESSED_DATA_PATH.glob("combined_club_elos_*.csv"))
    if not files:
        raise FileNotFoundError("No combined club ELOs file found! Run format_club_elos.py first.")
    return max(files, key=lambda x: x.stat().st_mtime)


def find_latest_match_logs_file() -> Path:
    """Find the most recent test match logs file."""
    files = list(PROCESSED_DATA_PATH.glob("test_match_logs_*.csv"))
    if not files:
        raise FileNotFoundError("No test match logs file found!")
    return max(files, key=lambda x: x.stat().st_mtime)


def load_club_elos() -> pd.DataFrame:
    """Load the combined club ELOs data."""
    elos_file = find_latest_combined_elos_file()
    logging.info(f"Loading club ELOs from {elos_file}")

    elos_df = pd.read_csv(elos_file)

    # Convert date columns to datetime
    elos_df["from_date"] = pd.to_datetime(elos_df["from_date"])
    elos_df["to_date"] = pd.to_datetime(elos_df["to_date"])

    # Sort by team and date for efficient lookup
    elos_df = elos_df.sort_values(["team_name", "league_name", "from_date"])

    logging.info(f"Loaded {len(elos_df)} ELO records for {elos_df['team_name'].nunique()} teams")
    return elos_df


def load_match_logs() -> pd.DataFrame:
    """Load the match logs data."""
    match_logs_file = find_latest_match_logs_file()
    logging.info(f"Loading match logs from {match_logs_file}")

    match_logs_df = pd.read_csv(match_logs_file)

    # Convert date column to datetime
    match_logs_df["date"] = pd.to_datetime(match_logs_df["date"])
    match_logs_df.loc[match_logs_df["league"] == "EPL", "league"] = "PremierLeague"

    logging.info(f"Loaded {len(match_logs_df)} match log records")
    return match_logs_df


def get_team_elo_at_date(
    team_name: str, league_name: str, match_date: pd.Timestamp, elos_df: pd.DataFrame
) -> Optional[float]:
    """
    Get the ELO rating for a team at a specific date.

    Args:
        team_name: Name of the team
        league_name: Name of the league
        match_date: Date of the match
        elos_df: DataFrame with ELO data

    Returns:
        ELO rating if found, None otherwise
    """
    # Filter ELOs for the specific team and league
    team_elos = elos_df[(elos_df["team_name"] == team_name) & (elos_df["league_name"] == league_name)].copy()

    if team_elos.empty:
        return None

    # Find ELO records that are valid at the match date
    # (match_date is between from_date and to_date, or match_date is after from_date if to_date is NaT)
    valid_elos = team_elos[
        (team_elos["from_date"] <= match_date)
        & ((team_elos["to_date"] >= match_date) | (pd.isna(team_elos["to_date"])))
    ]

    if not valid_elos.empty:
        # If we have an exact match, return the most recent one
        return valid_elos.iloc[-1]["elo"]

    # If no exact match, find the most recent ELO before the match date
    previous_elos = team_elos[team_elos["from_date"] <= match_date]

    if not previous_elos.empty:
        return previous_elos.iloc[-1]["elo"]

    # If no ELO found before the match date, return None
    return None


def add_club_elos_to_match_logs(
    match_logs_file: Optional[str] = None, elos_file: Optional[str] = None, output_file: Optional[str] = None
) -> None:
    """
    Add club ELO ratings to match logs.

    Args:
        match_logs_file: Path to match logs CSV file (if None, uses latest)
        elos_file: Path to combined ELOs CSV file (if None, uses latest)
        output_file: Optional custom output filename (without extension)
    """
    try:
        # Load data
        if elos_file:
            elos_df = pd.read_csv(elos_file)
            elos_df["from_date"] = pd.to_datetime(elos_df["from_date"])
            elos_df["to_date"] = pd.to_datetime(elos_df["to_date"])
            elos_df = elos_df.sort_values(["team_name", "league_name", "from_date"])
            logging.info(f"Loaded club ELOs from {elos_file}")
        else:
            elos_df = load_club_elos()

        if match_logs_file:
            match_logs_df = pd.read_csv(match_logs_file)
            match_logs_df["date"] = pd.to_datetime(match_logs_df["date"])
            logging.info(f"Loaded match logs from {match_logs_file}")
        else:
            match_logs_df = load_match_logs()

        # Check for required columns
        required_match_cols = ["date", "team", "league"]
        missing_cols = [col for col in required_match_cols if col not in match_logs_df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in match logs: {missing_cols}")

        required_elo_cols = ["team_name", "league_name", "from_date", "to_date", "elo"]
        missing_elo_cols = [col for col in required_elo_cols if col not in elos_df.columns]
        if missing_elo_cols:
            raise ValueError(f"Missing required columns in ELOs data: {missing_elo_cols}")

        # Add ELO ratings
        logging.info("Adding ELO ratings to match logs...")

        successful_matches = 0
        failed_matches = 0

        # Create a unique list of team-league-date combinations to avoid redundant lookups
        unique_combinations = match_logs_df[["team", "league", "date"]].drop_duplicates()
        elo_lookup_cache = {}
        # This is to handle the fact that we have some rows with SSC Napoli and others with Napoli
        additional_team_mappings = {
            "Napoli": "SSC Napoli",
        }

        logging.info(f"Processing {len(unique_combinations)} unique team-league-date combinations...")

        for _, row in tqdm(unique_combinations.iterrows(), total=len(unique_combinations), desc="Processing elos"):
            team_name = row["team"]
            league_name = row["league"]
            match_date = row["date"]

            cache_key = (team_name, league_name, match_date)

            if team_name in additional_team_mappings:
                team_name_to_lookup = additional_team_mappings[team_name]
            else:
                team_name_to_lookup = team_name

            team_elo = get_team_elo_at_date(team_name_to_lookup, league_name, match_date, elos_df)
            elo_lookup_cache[cache_key] = team_elo

            if team_elo is not None:
                successful_matches += 1
            else:
                failed_matches += 1

        # Apply the cached results to all match logs
        match_logs_df["team_elo_from_club_elo"] = match_logs_df.apply(
            lambda row: elo_lookup_cache.get((row["team"], row["league"], row["date"]), None), axis=1
        )

        # Calculate statistics
        total_records = len(match_logs_df)
        records_with_elo = match_logs_df["team_elo_from_club_elo"].notna().sum()

        logging.info(
            f"Successfully matched ELO ratings for {records_with_elo}/{total_records} records \
                ({(records_with_elo/total_records)*100:.1f}%)"
        )

        # Log some statistics about unmatched records
        unmatched_records = match_logs_df[match_logs_df["team_elo_from_club_elo"].isna()]
        if not unmatched_records.empty:
            unmatched_teams = unmatched_records[["team", "league"]].drop_duplicates()
            logging.warning(f"Could not find ELO ratings for {len(unmatched_teams)} team-league combinations:")
            for _, team_info in unmatched_teams.head(10).iterrows():  # Show first 10
                logging.warning(f"  - {team_info['team']} ({team_info['league']})")
            if len(unmatched_teams) > 10:
                logging.warning(f"  ... and {len(unmatched_teams) - 10} more")

        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d")
        if output_file:
            output_filename = f"{output_file}.csv"
        else:
            output_filename = f"test_match_logs_with_elos_{timestamp}.csv"

        output_path = PROCESSED_DATA_PATH / output_filename

        # Save enhanced match logs
        match_logs_df.to_csv(output_path, index=False)

        # Generate summary
        elo_stats = match_logs_df["team_elo_from_club_elo"].describe()

        logging.info("=" * 60)
        logging.info("ELO ADDITION COMPLETED SUCCESSFULLY")
        logging.info("=" * 60)
        logging.info(f"Output file: {output_path}")
        logging.info(f"Total records: {total_records:,}")
        logging.info(f"Records with ELO: {records_with_elo:,} ({(records_with_elo/total_records)*100:.1f}%)")
        logging.info(f"Records without ELO: {total_records - records_with_elo:,}")
        logging.info("\nELO rating statistics:")
        logging.info(f"  Mean: {elo_stats['mean']:.1f}")
        logging.info(f"  Min: {elo_stats['min']:.1f}")
        logging.info(f"  Max: {elo_stats['max']:.1f}")
        logging.info(f"  Std: {elo_stats['std']:.1f}")

    except Exception as e:
        logging.error(f"Error adding club ELOs to match logs: {str(e)}")
        raise


if __name__ == "__main__":
    fire.Fire(add_club_elos_to_match_logs)
