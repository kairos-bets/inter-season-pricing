import csv
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from pricing.models.transfermarkt import TransfermarktClub, TransfermarktTransfer, TransfermarktTransferMapped


def load_transfers(file_path: str) -> pd.DataFrame:
    """
    Load and validate the transfers
    """
    # Load the data row by row while validating the data
    transfers: list[TransfermarktTransfer] = []
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                transfer = TransfermarktTransfer(**row)
                transfers.append(transfer)
            except Exception as e:
                print(f"Error loading transfer: {e}")
                continue
    df_transfers = pd.DataFrame([transfer.model_dump() for transfer in transfers])
    df_transfers["transfer_date"] = pd.to_datetime(df_transfers["transfer_date"])
    return df_transfers


def load_clubs(file_path: str) -> pd.DataFrame:
    """
    Load and validate the clubs
    """
    clubs: list[TransfermarktClub] = []
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                club = TransfermarktClub(**row)
                clubs.append(club)
            except Exception as e:
                print(f"Error loading club: {e}")
                continue
    return pd.DataFrame([club.model_dump() for club in clubs])


def get_relevant_clubs(df_clubs: pd.DataFrame) -> pd.DataFrame:
    """
    Get the relevant clubs based on the following criteria:
    - Top 5 European leagues
    """
    top5_eu_competitions = ["FR1", "GB1", "ES1", "IT1", "L1"]
    return df_clubs.loc[df_clubs["domestic_competition_id"].isin(top5_eu_competitions), :]


def get_relevant_transfers(df_transfers: pd.DataFrame, df_clubs: pd.DataFrame) -> pd.DataFrame:
    """
    Get the relevant transfers based on the following criteria:
    - Transfer season
    - The destination club is in the top 5 European leagues
    """
    cutoff_date = datetime(2025, 4, 1)
    transfer_seasons_to_keep = ["21/22", "22/23", "23/24", "24/25"]
    df_transfers = df_transfers.loc[df_transfers["transfer_season"].isin(transfer_seasons_to_keep), :]
    df_transfers = df_transfers.loc[df_transfers["to_club_id"].isin(list(df_clubs["club_id"].unique())), :]
    df_transfers = df_transfers.loc[df_transfers["transfer_date"] <= cutoff_date, :]
    # remove duplicate transfers for a player (lent then bought)
    df_transfers.drop_duplicates(subset=["transfer_season", "from_club_id", "to_club_id", "player_name"], inplace=True)
    return df_transfers


def map_player_and_club_names_from_transfermarkt_to_fbref(
    df_transfers: pd.DataFrame, df_matches: pd.DataFrame, club_name_mapping: dict[str, str]
) -> pd.DataFrame:
    """
    Map the player and club names from transfermarkt to fbref
    """
    df_transfers["player_name_mapped"] = df_transfers["player_name"].apply(
        lambda x: df_matches.loc[x, "df2_name"] if x in df_matches.index else x
    )
    df_transfers["from_club_name_mapped"] = df_transfers["from_club_name"].apply(
        lambda x: club_name_mapping[x] if x in club_name_mapping else x
    )
    df_transfers["to_club_name_mapped"] = df_transfers["to_club_name"].apply(
        lambda x: club_name_mapping[x] if x in club_name_mapping else x
    )
    return df_transfers


def add_club_domestic_competition_id(df_transfers: pd.DataFrame, df_clubs: pd.DataFrame) -> pd.DataFrame:
    """
    Add the club domestic competition id to the transfers
    """
    df_transfers = df_transfers.copy()
    df_clubs.index = df_clubs["club_id"]
    df_transfers["from_club_domestic_competition_id"] = df_transfers["from_club_id"].map(
        df_clubs["domestic_competition_id"]
    )
    df_transfers["to_club_domestic_competition_id"] = df_transfers["to_club_id"].map(
        df_clubs["domestic_competition_id"]
    )
    return df_transfers


def load_transfers_mapped_names(file_path: str) -> pd.DataFrame:
    transfers_mapped_names: list[TransfermarktTransferMapped] = []
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                transfer = TransfermarktTransferMapped(**row)
                transfers_mapped_names.append(transfer)
            except Exception as e:
                print(f"Error loading transfer mapped: {e}")
                continue
    df_transfers_mapped_names = pd.DataFrame([transfer.model_dump() for transfer in transfers_mapped_names])
    df_transfers_mapped_names["transfer_date"] = pd.to_datetime(df_transfers_mapped_names["transfer_date"])
    return df_transfers_mapped_names


def find_latest_transfers_mapped_file(processed_data_path: Path) -> Optional[Path]:
    """Find the most recent transfers mapped file"""
    transfers_mapped_files = list(processed_data_path.glob("transfers_relevant_mapped_*.csv"))
    if not transfers_mapped_files:
        return None
    return max(transfers_mapped_files, key=lambda x: x.stat().st_mtime)
