"""
Format club ELOs that were created with get_team_elos_from_club_elo.py
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import fire
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Path constants
DATA_PATH = Path(__file__).parent.parent.parent.parent / "data"
CLUB_ELO_PATH = DATA_PATH / "club-elo"
PROCESSED_DATA_PATH = DATA_PATH / "processed"


def find_latest_team_names_file() -> Path:
    """Find the most recent team names file."""
    files = list(PROCESSED_DATA_PATH.glob("team_names_*_manual.csv"))
    if not files:
        raise FileNotFoundError("No team names file found!")
    return max(files, key=lambda x: x.stat().st_mtime)


def extract_normalized_name_from_filename(filename: str) -> str:
    """
    Extract normalized team name from filename.
    Expected format: {normalized_name}_{date}.csv
    """
    # Remove the .csv extension and split by underscore
    name_parts = filename.replace(".csv", "").split("_")
    # Remove the last part (date) and join the rest
    return "_".join(name_parts[:-1])


def load_team_mapping() -> pd.DataFrame:
    """Load the team names mapping file."""
    team_names_file = find_latest_team_names_file()
    logging.info(f"Loading team mapping from {team_names_file}")
    return pd.read_csv(team_names_file)


def read_and_combine_club_elos() -> pd.DataFrame:
    """
    Read all club ELO CSV files and combine them into a single DataFrame.
    """
    # Get all CSV files in club-elo directory
    csv_files = list(CLUB_ELO_PATH.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {CLUB_ELO_PATH}")

    logging.info(f"Found {len(csv_files)} club ELO files to process")

    combined_data = []

    for csv_file in csv_files:
        try:
            # Extract normalized team name from filename
            normalized_name = extract_normalized_name_from_filename(csv_file.name)

            # Read the CSV file
            df = pd.read_csv(csv_file)

            # Add normalized_team_name column
            df["normalized_team_name"] = normalized_name

            combined_data.append(df)

        except Exception as e:
            logging.warning(f"Error processing file {csv_file}: {str(e)}")
            continue

    if not combined_data:
        raise ValueError("No valid data found in any CSV files")

    # Combine all DataFrames
    combined_df = pd.concat(combined_data, ignore_index=True)
    logging.info(f"Combined {len(combined_df)} ELO records from {len(combined_data)} teams")

    return combined_df


def add_team_information(elo_df: pd.DataFrame, team_mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Add team_name and league_name columns using the team mapping.
    """
    # Merge with team mapping
    merged_df = elo_df.merge(
        team_mapping[["normalized_team_name", "team_name", "league_name"]], on="normalized_team_name", how="left"
    )

    # Check for teams that couldn't be mapped
    unmapped_teams = merged_df[merged_df["team_name"].isna()]["normalized_team_name"].unique()
    if len(unmapped_teams) > 0:
        logging.warning(f"Could not map {len(unmapped_teams)} teams: {list(unmapped_teams)}")

    logging.info(
        f"Successfully mapped {len(merged_df) - merged_df['team_name'].isna().sum()} out of {len(merged_df)} records"
    )

    return merged_df


def filter_recent_data(df: pd.DataFrame, start_year: int = 2020) -> pd.DataFrame:
    """
    Filter data to keep only records from start_year onwards.
    """
    # Convert from_date to datetime
    df["from_date"] = pd.to_datetime(df["from_date"])

    # Filter data from start_year onwards
    cutoff_date = pd.Timestamp(f"{start_year}-01-01")
    filtered_df = df[df["from_date"] >= cutoff_date].copy()

    logging.info(
        f"Filtered data: kept {len(filtered_df)} records from {start_year} onwards \
            (removed {len(df) - len(filtered_df)} older records)"
    )

    return filtered_df


def format_club_elos(output_file: Optional[str] = None) -> None:
    """
    Main function to format and combine all club ELO data.

    Args:
        output_file: Optional custom output filename (without extension)
    """
    try:
        # Load team mapping
        team_mapping = load_team_mapping()

        # Read and combine all club ELO files
        combined_elos = read_and_combine_club_elos()

        # Add team information
        elos_with_teams = add_team_information(combined_elos, team_mapping)

        # Filter data from 2020 onwards
        recent_elos = filter_recent_data(elos_with_teams, start_year=2020)

        # Remove records where team mapping failed
        recent_elos = recent_elos.dropna(subset=["team_name", "league_name"])

        # Reorder columns for better readability
        column_order = [
            "normalized_team_name",
            "team_name",
            "league_name",
            "rank",
            "club",
            "country",
            "level",
            "elo",
            "from_date",
            "to_date",
        ]
        recent_elos = recent_elos[column_order]

        # Sort by team name and date
        recent_elos = recent_elos.sort_values(["team_name", "from_date"])

        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d")
        if output_file:
            output_filename = f"{output_file}.csv"
        else:
            output_filename = f"combined_club_elos_{timestamp}.csv"

        output_path = PROCESSED_DATA_PATH / output_filename

        # Save to CSV
        recent_elos.to_csv(output_path, index=False)

        # Generate summary statistics
        summary = {
            "total_records": len(recent_elos),
            "unique_teams": recent_elos["team_name"].nunique(),
            "unique_leagues": recent_elos["league_name"].nunique(),
            "date_range": f"{recent_elos['from_date'].min().strftime('%Y-%m-%d')} to \
                {recent_elos['from_date'].max().strftime('%Y-%m-%d')}",
            "teams_by_league": recent_elos.groupby("league_name")["team_name"].nunique().to_dict(),
        }

        logging.info("=" * 60)
        logging.info("FORMATTING COMPLETED SUCCESSFULLY")
        logging.info("=" * 60)
        logging.info(f"Output file: {output_path}")
        logging.info(f"Total records: {summary['total_records']:,}")
        logging.info(f"Unique teams: {summary['unique_teams']}")
        logging.info(f"Unique leagues: {summary['unique_leagues']}")
        logging.info(f"Date range: {summary['date_range']}")
        logging.info("\nTeams per league:")
        for league, count in summary["teams_by_league"].items():
            logging.info(f"  {league}: {count} teams")

    except Exception as e:
        logging.error(f"Error formatting club ELOs: {str(e)}")
        raise


if __name__ == "__main__":
    fire.Fire(format_club_elos)
