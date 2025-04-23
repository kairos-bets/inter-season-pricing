import csv
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

from pricing.models.fbref import MatchElo, PlayerMatchLogs, PostTransferPlayerMatchLogs


def load_match_logs(file_path: str) -> pd.DataFrame:
    """
    Load and validate player match logs from fbref
    """
    # Load the data row by row while validating with Pydantic model
    match_logs: list[PlayerMatchLogs] = []
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                log = PlayerMatchLogs(**row)
                match_logs.append(log)
            except Exception as e:
                print(f"Error loading match log: {e}")
                continue

    # Convert to DataFrame
    df_match_logs = pd.DataFrame([log.model_dump() for log in match_logs])
    df_match_logs = df_match_logs.loc[df_match_logs["date"] != "Date", :]
    df_match_logs = df_match_logs.loc[df_match_logs["date"].notna(), :]

    # Convert date to datetime if not None
    df_match_logs["date"] = pd.to_datetime(df_match_logs["date"], errors="coerce")

    return df_match_logs


def get_post_transfer_match_logs(
    df_match_logs: pd.DataFrame, transfers_mapped_names: pd.DataFrame, number_of_post_transfer_matches: int = 10
) -> pd.DataFrame | None:
    """
    Get the post-transfer match logs for the players in the transfers file
    """
    match_logs_post_transfer = []

    for player_name, player_transfers in tqdm(
        transfers_mapped_names.groupby("player_name_mapped"), total=len(transfers_mapped_names)
    ):
        player_transfers = player_transfers.sort_values("transfer_date")
        player_matches = df_match_logs.loc[df_match_logs["player_name"] == player_name, :]

        if player_matches.empty:
            continue

        # Process each transfer for this player
        for i, transfer in player_transfers.iterrows():
            to_club_name = transfer["to_club_name_mapped"]
            transfer_date = transfer["transfer_date"]
            next_transfers = player_transfers.loc[player_transfers["transfer_date"] > transfer_date, :]
            next_transfer_date = next_transfers.iloc[0]["transfer_date"] if not next_transfers.empty else None

            matches_filter = (player_matches["date"] >= transfer_date) & (player_matches["team"] == to_club_name)

            if next_transfer_date is not None:
                matches_filter &= player_matches["date"] < next_transfer_date

            matches_at_new_club = player_matches.loc[matches_filter, :].sort_values("date")
            first_n_matches = matches_at_new_club.iloc[:number_of_post_transfer_matches].copy()

            if not first_n_matches.empty:
                # Add transfer information to the matches
                first_n_matches["transfer_id"] = (
                    player_name
                    + "_"
                    + str(transfer["from_club_name_mapped"])
                    + "_"
                    + str(transfer["to_club_name_mapped"])
                    + "_"
                    + transfer_date.strftime("%Y-%m-%d")
                )
                first_n_matches["transfer_date"] = transfer_date
                first_n_matches["from_club"] = transfer["from_club_name_mapped"]
                first_n_matches["to_club"] = to_club_name
                first_n_matches["match_number_after_transfer"] = range(1, len(first_n_matches) + 1)
                first_n_matches["days_since_transfer"] = (first_n_matches["date"] - transfer_date).dt.days

                match_logs_post_transfer.append(first_n_matches)

    if match_logs_post_transfer:
        return pd.concat(match_logs_post_transfer, ignore_index=True)

    return None


def find_latest_post_transfer_file(processed_data_path: Path) -> Optional[Path]:
    """Find the most recent post-transfer match logs file"""
    post_transfer_files = list(processed_data_path.glob("post_transfer_match_logs_*.csv"))
    if not post_transfer_files:
        return None
    return max(post_transfer_files, key=lambda x: x.stat().st_mtime)


def load_post_transfer_match_logs(file_path: str) -> pd.DataFrame:
    """
    Load and validate post-transfer player match logs
    """
    match_logs: list[PostTransferPlayerMatchLogs] = []
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                log = PostTransferPlayerMatchLogs(**row)
                match_logs.append(log)
            except Exception as e:
                print(f"Error loading match log: {e}")
                continue

    df_match_logs = pd.DataFrame([log.model_dump() for log in match_logs])
    df_match_logs["date"] = pd.to_datetime(df_match_logs["date"], errors="coerce")
    df_match_logs["transfer_date"] = pd.to_datetime(df_match_logs["transfer_date"], errors="coerce")

    return df_match_logs


def create_match_id(
    df: pd.DataFrame, first_team_column: str, second_team_column: str, date_column: str
) -> pd.DataFrame:
    """
    Create a unique match id for each match.
    The match id is the concatenation of the sorted team names and the date.
    """
    df = df.copy()
    df["match_id"] = df.apply(
        lambda row: "_".join(
            sorted([row[first_team_column], row[second_team_column]]) + [row[date_column].strftime("%Y-%m-%d")]
        ),
        axis=1,
    )
    return df


def create_player_match_id(df: pd.DataFrame, player_id_column: str, match_id_column: str) -> pd.DataFrame:
    """
    Create a unique player match id for each match.
    The player match id is the concatenation of the player id and the match id.
    """
    df = df.copy()
    df["player_match_id"] = df[player_id_column] + "_" + df[match_id_column]
    return df


def load_elo_data(file_path: str) -> pd.DataFrame:
    """
    Load and validate ELO data from CSV file
    """
    elo_data: list[MatchElo] = []
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                data = MatchElo(**row)
                elo_data.append(data)
            except Exception as e:
                print(f"Error loading ELO data: {e}")
                continue

    df_elo = pd.DataFrame([data.model_dump() for data in elo_data])
    df_elo["date"] = pd.to_datetime(df_elo["date"], errors="coerce")

    return df_elo


def merge_elo_data(df_matches: pd.DataFrame, df_elo: pd.DataFrame) -> pd.DataFrame:
    """
    Merge ELO data with match data based on match_id
    and add team-specific ELO metrics based on whether team was home or away
    """
    # Create match_id in ELO data to match the format in match logs
    df_elo = create_match_id(df_elo, "home_team", "away_team", "date")

    # Merge on match_id
    df_result = pd.merge(df_matches, df_elo, on="match_id", how="left")

    # Determine if team was home or away
    df_result["is_home"] = df_result["team"] == df_result["home_team"]

    return df_result
