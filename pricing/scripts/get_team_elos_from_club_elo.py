"""
Fetch entire historical team elos from the club elo API
(http://api.clubelo.com/TEAM_NAME)
"""

import io
import logging
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import List

import fire
import pandas as pd
import requests
from tqdm import tqdm

from pricing.models.club_elo import ClubElo

DATA_PATH = Path(__file__).parent.parent.parent / "data"
PROCESSED_DATA_PATH = DATA_PATH / "processed" / "final"
CLUB_ELO_PATH = DATA_PATH / "club-elo" / "final"

# Ensure the club-elo directory exists
CLUB_ELO_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def find_latest_team_names_file() -> Path:
    """Find the most recent team names file."""
    files = list(PROCESSED_DATA_PATH.glob("team_names_*_manual.csv"))
    if not files:
        raise FileNotFoundError("No team names file found! Run get_team_names.py first.")
    return max(files, key=lambda x: x.stat().st_mtime)


def fetch_club_elo(team_name: str) -> pd.DataFrame:
    """
    Fetch historical ELO ratings for a team from the Club Elo API.

    Args:
        team_name: The normalized team name to query

    Returns:
        DataFrame with the ELO data or empty DataFrame if request fails
    """
    url = f"http://api.clubelo.com/{team_name}"

    try:
        response = requests.get(url, timeout=180)
        response.raise_for_status()  # Raise an exception for 4XX/5XX responses

        # If successful, convert to DataFrame
        if response.text and "404 page not found" not in response.text:
            df = pd.read_csv(io.StringIO(response.text))
            return df
        else:
            logging.warning(f"No data returned for team: {team_name}")
            return pd.DataFrame()

    except requests.RequestException as e:
        logging.error(f"Error fetching ELO data for {team_name}: {str(e)}")
        return pd.DataFrame()


def validate_and_convert_elo_data(df: pd.DataFrame) -> List[ClubElo]:
    """
    Validate and convert ELO data to ClubElo Pydantic models.

    Args:
        df: DataFrame with ELO data from API

    Returns:
        List of validated ClubElo objects
    """
    validated_elos = []

    # Map DataFrame columns to ClubElo model fields
    for _, row in df.iterrows():
        try:
            # Handle None in Rank field
            rank = row.get("Rank")
            if pd.isna(rank) or rank == "None":
                rank = 0  # Default value for missing ranks

            # Create ClubElo object with proper field mapping
            elo_obj = ClubElo(
                rank=rank,
                club=row["Club"],
                country=row["Country"],
                level=row["Level"],
                elo=row["Elo"],
                from_date=row["From"],
                to_date=row["To"],
            )
            validated_elos.append(elo_obj)
        except Exception as e:
            logging.warning(f"Error validating ELO data row: {row}\nError: {str(e)}")
            continue

    return validated_elos


def elo_models_to_dataframe(elo_models: List[ClubElo]) -> pd.DataFrame:
    """Convert a list of ClubElo models back to a DataFrame."""
    if not elo_models:
        return pd.DataFrame()

    data = []
    for model in elo_models:
        data.append(model.model_dump())

    return pd.DataFrame(data)


def get_team_elos(team_names_file: str = None, delay: float = 0.5) -> None:
    """
    Fetch historical ELO ratings for all teams in the team names file.

    Args:
        team_names_file: Path to the team names CSV file (if None, uses latest)
        delay: Delay between API requests in seconds to avoid overwhelming the API
    """
    # Find latest team names file if not specified
    if not team_names_file:
        team_names_file = str(find_latest_team_names_file())

    logging.info(f"Loading team names from {team_names_file}")
    teams_df = pd.read_csv(team_names_file)

    timestamp = datetime.now().strftime("%Y%m%d")
    success_count = 0
    error_count = 0
    validation_errors = 0
    skipped_count = 0

    # Create a log file for tracking API results
    log_path = CLUB_ELO_PATH / f"api_results_{timestamp}.txt"
    with open(log_path, "w") as log_file:
        log_file.write(f"Club Elo API Results - {datetime.now()}\n")
        log_file.write("=" * 60 + "\n\n")

        # Process each team
        for idx, row in tqdm(teams_df.iterrows(), total=len(teams_df), desc="Fetching Club Elo data"):
            team_name = row["team_name"]
            normalized_name = row["normalized_team_name"]
            league_name = row["league_name"]

            log_file.write(f"Team: {team_name} ({league_name})\n")
            log_file.write(f"Normalized name: {normalized_name}\n")

            # Check if file already exists for today
            output_file = CLUB_ELO_PATH / f"{normalized_name}_{timestamp}.csv"
            if output_file.exists():
                skipped_count += 1
                log_file.write(f"SKIPPED: File {output_file.name} already exists\n")
                log_file.write("-" * 40 + "\n")
                continue

            # Fetch ELO data using normalized name
            elo_data_df = fetch_club_elo(normalized_name)

            if not elo_data_df.empty:
                # Validate and convert to ClubElo models
                validated_elos = validate_and_convert_elo_data(elo_data_df)

                if validated_elos:
                    # Convert validated models back to DataFrame for saving
                    validated_df = elo_models_to_dataframe(validated_elos)

                    # Save to CSV
                    validated_df.to_csv(output_file, index=False)
                    success_count += 1
                    log_file.write(
                        f"SUCCESS: Validated {len(validated_elos)} ELO records and saved to {output_file.name}\n"
                    )
                else:
                    validation_errors += 1
                    log_file.write("VALIDATION ERROR: Failed to validate any ELO records\n")
            else:
                error_count += 1
                log_file.write("ERROR: Failed to retrieve data\n")

            log_file.write("-" * 40 + "\n")

            # Add delay to avoid overwhelming the API
            sleep(delay)

    # Generate a summary file
    with open(CLUB_ELO_PATH / f"summary_{timestamp}.txt", "w") as summary_file:
        summary_file.write(f"Club Elo API Summary - {datetime.now()}\n")
        summary_file.write("=" * 60 + "\n\n")
        summary_file.write(f"Total teams processed: {len(teams_df)}\n")
        summary_file.write(f"Successful retrievals: {success_count}\n")
        summary_file.write(f"Failed retrievals: {error_count}\n")
        summary_file.write(f"Validation errors: {validation_errors}\n")
        summary_file.write(f"Skipped (already exists): {skipped_count}\n")
        summary_file.write(f"Success rate: {(success_count / len(teams_df)) * 100:.2f}%\n")

    logging.info(f"Processed {len(teams_df)} teams")
    logging.info(f"Successfully retrieved and validated ELO data for {success_count} teams")
    logging.info(f"Failed to retrieve ELO data for {error_count} teams")
    logging.info(f"Failed validation for {validation_errors} teams")
    logging.info(f"Skipped {skipped_count} teams (files already exist)")
    logging.info(f"Results saved to {CLUB_ELO_PATH}")


if __name__ == "__main__":
    fire.Fire(get_team_elos)
