import csv

import pandas as pd
from tqdm import tqdm

from pricing.models.fbref import PlayerMatchLogs


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
                    + str(transfer_date)
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
