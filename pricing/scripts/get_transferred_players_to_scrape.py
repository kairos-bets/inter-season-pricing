"""
This script gets information about the players that have been transferred so that we can run
the correct scraping pipeline.
Information includes player name, etc. but also a normalized ID of the club they moved from.
We have the post-transfer match logs from our match logs of the top 5 EU leagues since they transferred to these leagues
but we need to get the information from the leagues they moved from.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import fire
import pandas as pd

from pricing.format.match_logs import find_latest_post_transfer_file, load_post_transfer_match_logs
from pricing.format.transfer import find_latest_transfers_mapped_file, load_transfers_mapped_names

DATA_PATH = Path(__file__).parent.parent.parent / "data"
PROCESSED_DATA_PATH = DATA_PATH / "processed"
MATCH_MAPPING_PATH = DATA_PATH / "match-transfermarkt-to-fbref"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_latest_post_transfer_match_logs() -> Optional[pd.DataFrame]:
    """Load the most recent post-transfer match logs file"""
    latest_file = find_latest_post_transfer_file(PROCESSED_DATA_PATH)
    if latest_file is None:
        logging.error("No post-transfer match logs found!")
        return None
    logging.info(f"Loading post-transfer match logs from {latest_file}")
    df = load_post_transfer_match_logs(str(latest_file))
    logging.info(f"Loaded {len(df)} post-transfer match logs")
    return df


def load_latest_transfers_mapped() -> Optional[pd.DataFrame]:
    """Load the most recent mapped transfers file"""
    latest_file = find_latest_transfers_mapped_file(PROCESSED_DATA_PATH)
    if latest_file is None:
        logging.error("No mapped transfer files found!")
        return None
    logging.info(f"Loading mapped transfers from {latest_file}")
    df = load_transfers_mapped_names(str(latest_file))
    logging.info(f"Loaded {len(df)} mapped transfers")
    return df


def load_competition_mappings() -> tuple[Dict, Dict]:
    """Load competition ID mappings"""
    # Load club name to competition ID mapping
    with open(MATCH_MAPPING_PATH / "club_name_to_competition_id.json", "r") as f:
        club_name_to_competition_id = json.load(f)

    # Load competition ID to FBref league name mapping
    with open(MATCH_MAPPING_PATH / "competition_id_to_fbref_league_name.json", "r") as f:
        competition_id_to_league_name = json.load(f)

    logging.info(
        f"Loaded mappings for {len(club_name_to_competition_id)} clubs and \
            {len(competition_id_to_league_name)} competitions"
    )
    return club_name_to_competition_id, competition_id_to_league_name


def get_players_to_scrape(
    df_post_transfer: pd.DataFrame,
    df_transfers: pd.DataFrame,
    club_name_to_competition_id: Dict,
    competition_id_to_league_name: Dict,
) -> pd.DataFrame:
    """
    Extract players who need to be scraped with one entry per transfer_id

    Args:
        df_post_transfer: DataFrame with post-transfer match logs
        df_transfers: DataFrame with mapped transfers
        club_name_to_competition_id: Mapping of club names to competition IDs
        competition_id_to_league_name: Mapping of competition IDs to league names

    Returns:
        DataFrame with players to scrape
    """
    # Get unique transfers from the post-transfer match logs
    df_players = df_post_transfer[
        ["player_name", "player_id", "transfer_id", "transfer_date", "from_club", "to_club", "league"]
    ].drop_duplicates()

    logging.info(f"Found {len(df_players)} unique transfers to process")

    # Create transfer_id column on the transfers data for easier merging
    df_transfers["transfer_id"] = (
        df_transfers["player_name_mapped"]
        + "_"
        + df_transfers["from_club_name_mapped"]
        + "_"
        + df_transfers["to_club_name_mapped"]
        + "_"
        + df_transfers["transfer_date"].astype(str)
    )
    df_transfers.rename(
        columns={"from_club_name": "from_club_name_transfermarkt", "to_club_name": "to_club_name_transfermarkt"},
        inplace=True,
    )
    df_players = pd.merge(
        df_players,
        df_transfers[
            [
                "transfer_id",
                "transfer_season",
                "from_club_name_transfermarkt",
                "to_club_name_transfermarkt",
                "from_club_domestic_competition_id",
                "to_club_domestic_competition_id",
            ]
        ],
        on="transfer_id",
        how="left",
    )

    # Try to get competition IDs for clubs that don't have them
    mask_missing_from_competition = df_players["from_club_domestic_competition_id"].isna()
    if mask_missing_from_competition.any():
        logging.info(f"Attempting to map {mask_missing_from_competition.sum()} missing from-club competition IDs")
        for idx in df_players[mask_missing_from_competition].index:
            club_name = df_players.loc[idx, "from_club_name_transfermarkt"]
            if club_name in club_name_to_competition_id:
                df_players.loc[idx, "from_club_domestic_competition_id"] = club_name_to_competition_id[club_name]

    # Map competition IDs to league names
    df_players["from_club_domestic_competition_name"] = (
        df_players["from_club_domestic_competition_id"].map(competition_id_to_league_name).fillna("")
    )

    df_players["to_club_domestic_competition_name"] = (
        df_players["to_club_domestic_competition_id"].map(competition_id_to_league_name).fillna("")
    )

    logging.info(f"Final dataset has {len(df_players)} players to scrape")

    return df_players[
        [
            "player_name",
            "player_id",
            "transfer_id",
            "transfer_date",
            "transfer_season",
            "from_club",
            "to_club",
            "from_club_name_transfermarkt",
            "to_club_name_transfermarkt",
            "league",
            "from_club_domestic_competition_id",
            "from_club_domestic_competition_name",
            "to_club_domestic_competition_id",
            "to_club_domestic_competition_name",
        ]
    ]


def get_transferred_players() -> None:
    """
    Main function to identify players to scrape and output the information to a CSV file
    """
    logging.info("Starting to identify players for scraping")

    # Load the necessary data

    df_post_transfer = load_latest_post_transfer_match_logs()
    df_transfers = load_latest_transfers_mapped()

    club_name_to_competition_id, competition_id_to_league_name = load_competition_mappings()

    # Get the players to scrape
    df_players_to_scrape = get_players_to_scrape(
        df_post_transfer=df_post_transfer,
        df_transfers=df_transfers,
        club_name_to_competition_id=club_name_to_competition_id,
        competition_id_to_league_name=competition_id_to_league_name,
    )

    # Filter out players that don't have a from_club_domestic_competition_id
    mask_missing_from_competition = df_players_to_scrape["from_club_domestic_competition_id"] == ""
    logging.info(
        f"Filtering out {mask_missing_from_competition.sum()} players without a from-club competition ID \
             from a total of {len(df_players_to_scrape)} players"
    )
    df_players_to_scrape = df_players_to_scrape[~mask_missing_from_competition]

    # Save the output
    timestamp = datetime.now().strftime("%Y%m%d")
    output_path = PROCESSED_DATA_PATH / f"players_to_scrape_{timestamp}.csv"
    logging.info(f"Saving players to scrape to {output_path}")

    df_players_to_scrape.to_csv(output_path, index=False)
    logging.info("Successfully identified players to scrape")


if __name__ == "__main__":
    fire.Fire(get_transferred_players)
