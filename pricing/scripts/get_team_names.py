"""
This script extracts team names and their leagues from our datasets.
It writes a CSV file with three columns: team_name, league_name, and normalized_team_name.
"""

import logging
from datetime import datetime
from pathlib import Path

import fire
import pandas as pd

DATA_PATH = Path(__file__).parent.parent.parent / "data"
PROCESSED_DATA_PATH = DATA_PATH / "processed"

# Define top 5 European leagues
TOP_5_LEAGUES = ["PremierLeague", "LaLiga", "Bundesliga", "SerieA", "Ligue1"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def normalize_name(name: str) -> str:
    """Normalize team or league name by lowercasing and removing whitespace."""
    if not isinstance(name, str):
        return ""
    return name.lower().replace(" ", "").replace(".", "")


def is_top_5_league(league_name: str) -> bool:
    """Check if a league is in the top 5 European leagues."""
    if not isinstance(league_name, str):
        return False

    normalized = normalize_name(league_name)
    return any(normalize_name(league) in normalized for league in TOP_5_LEAGUES)


def find_latest_file(pattern: str) -> Path:
    """Find the most recent file matching the given pattern."""
    files = list(PROCESSED_DATA_PATH.glob(pattern))
    if not files:
        return None
    return max(files, key=lambda x: x.stat().st_mtime)


def extract_teams_from_players_to_scrape() -> pd.DataFrame:
    """Extract team names and leagues from the players_to_scrape file."""
    logging.info("Extracting teams from players_to_scrape file")

    # Find latest players_to_scrape file
    latest_file = find_latest_file("players_to_scrape_*.csv")
    if latest_file is None:
        logging.error("No players_to_scrape file found!")
        return pd.DataFrame()

    # Load players_to_scrape file
    players_df = pd.read_csv(latest_file)

    # Extract team and league names
    teams_data = []

    # From clubs
    from_clubs = players_df[["from_club", "from_club_domestic_competition_name"]].drop_duplicates()
    for _, row in from_clubs.iterrows():
        if pd.notna(row["from_club"]) and pd.notna(row["from_club_domestic_competition_name"]):
            teams_data.append(
                {
                    "team_name": row["from_club"],
                    "league_name": row["from_club_domestic_competition_name"],
                    "normalized_team_name": normalize_name(row["from_club"]),
                }
            )

    # To clubs
    to_clubs = players_df[["to_club", "to_club_domestic_competition_name"]].drop_duplicates()
    for _, row in to_clubs.iterrows():
        if pd.notna(row["to_club"]) and pd.notna(row["to_club_domestic_competition_name"]):
            teams_data.append(
                {
                    "team_name": row["to_club"],
                    "league_name": row["to_club_domestic_competition_name"],
                    "normalized_team_name": normalize_name(row["to_club"]),
                }
            )

    # Create DataFrame and drop duplicates
    teams_df = pd.DataFrame(teams_data).drop_duplicates(subset=["team_name", "league_name"])
    logging.info(f"Extracted {len(teams_df)} teams from players_to_scrape file")

    return teams_df


def extract_teams_from_match_logs() -> pd.DataFrame:
    """Extract team names and leagues from the train and test match logs."""
    logging.info("Extracting teams from match logs")

    teams_data = []

    # Find latest training match logs file
    train_file = find_latest_file("training_match_logs_*.csv")
    if train_file is not None:
        logging.info(f"Loading training match logs from {train_file.name}")
        # Read only necessary columns to save memory
        train_df = pd.read_csv(train_file, usecols=["team", "league"])
        train_teams = train_df[["team", "league"]].drop_duplicates()

        for _, row in train_teams.iterrows():
            if pd.notna(row["team"]) and pd.notna(row["league"]):
                teams_data.append(
                    {
                        "team_name": row["team"],
                        "league_name": row["league"],
                        "normalized_team_name": normalize_name(row["team"]),
                    }
                )

        logging.info(f"Extracted {len(train_teams)} teams from training match logs")

    # Find latest test match logs file
    test_file = find_latest_file("test_match_logs_*.csv")
    if test_file is not None:
        logging.info(f"Loading test match logs from {test_file.name}")
        # Read only necessary columns to save memory
        test_df = pd.read_csv(test_file, usecols=["team", "league"])
        test_teams = test_df[["team", "league"]].drop_duplicates()

        for _, row in test_teams.iterrows():
            if pd.notna(row["team"]) and pd.notna(row["league"]):
                teams_data.append(
                    {
                        "team_name": row["team"],
                        "league_name": row["league"],
                        "normalized_team_name": normalize_name(row["team"]),
                    }
                )

        logging.info(f"Extracted {len(test_teams)} teams from test match logs")

    # Create DataFrame and drop duplicates
    teams_df = pd.DataFrame(teams_data).drop_duplicates(subset=["team_name", "league_name"])

    return teams_df


def get_team_names() -> None:
    """
    Extract team names and leagues from datasets, normalize them, and save to CSV.
    Teams from top 5 European leagues are prioritized in the sorting.
    """
    logging.info("Starting team name extraction")

    # Extract teams from players_to_scrape file
    players_teams_df = extract_teams_from_players_to_scrape()

    # Extract teams from match logs
    match_logs_teams_df = extract_teams_from_match_logs()

    # Combine datasets
    all_teams_df = pd.concat([players_teams_df, match_logs_teams_df], ignore_index=True)

    # Map "EPL" to "PremierLeague" to normalize league naming
    all_teams_df.loc[all_teams_df["league_name"] == "EPL", "league_name"] = "PremierLeague"

    # Drop duplicates
    all_teams_df = all_teams_df.drop_duplicates(subset=["team_name", "league_name"])

    # Add column to indicate if team is in top 5 league
    all_teams_df["is_top_5"] = all_teams_df["league_name"].apply(is_top_5_league)

    # Sort by top 5 leagues first, then by team name
    all_teams_df = all_teams_df.sort_values(by=["is_top_5", "league_name", "team_name"], ascending=[False, True, True])

    # Drop the is_top_5 column as it's no longer needed
    all_teams_df = all_teams_df[["team_name", "league_name", "normalized_team_name"]]

    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d")
    output_path = PROCESSED_DATA_PATH / f"team_names_{timestamp}.csv"
    all_teams_df.to_csv(output_path, index=False)

    logging.info(f"Extracted {len(all_teams_df)} unique team names")
    logging.info(f"Saved team names to {output_path}")


if __name__ == "__main__":
    fire.Fire(get_team_names)
