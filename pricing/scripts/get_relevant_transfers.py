"""
This script filters the relevant transfers from the transfermarkt data based on various criteria
"""

import logging
import pickle
from datetime import datetime
from pathlib import Path

import fire
import pandas as pd

from pricing.format.transfer import (
    add_club_domestic_competition_id,
    get_relevant_transfers,
    load_clubs,
    load_transfers,
    map_player_and_club_names_from_transfermarkt_to_fbref,
)

DATA_PATH = Path(__file__).parent.parent.parent / "data"
TRANSFERMARKT_DATA_PATH = DATA_PATH / "transfermarkt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_transfers() -> None:
    logging.info("Starting transfer data processing")

    logging.info(f"Loading transfers from {TRANSFERMARKT_DATA_PATH / 'transfers.csv'}")
    df_transfers = load_transfers(TRANSFERMARKT_DATA_PATH / "transfers.csv")
    logging.info(f"Loaded {len(df_transfers)} transfers")

    logging.info(f"Loading clubs from {TRANSFERMARKT_DATA_PATH / 'clubs.csv'}")
    df_clubs = load_clubs(TRANSFERMARKT_DATA_PATH / "clubs.csv")
    logging.info(f"Loaded {len(df_clubs)} clubs")

    logging.info("Filtering relevant transfers...")
    df_transfers = get_relevant_transfers(df_transfers, df_clubs)
    logging.info(f"Filtered to {len(df_transfers)} relevant transfers")

    timestamp = datetime.now().strftime("%Y%m%d")
    output_path = DATA_PATH / "processed" / f"transfers_relevant_{timestamp}.csv"
    logging.info(f"Saving filtered transfers to {output_path}")

    df_transfers.to_csv(output_path, index=False)
    logging.info("First filtering completed successfully")

    logging.info("Now mapping player and club names from transfermarkt to fbref...")
    df_name_matches = pd.read_csv(DATA_PATH / "match-transfermarkt-to-fbref" / "player_name_matches_final.csv")
    df_name_matches.index = df_name_matches["df1_name"]

    with open(DATA_PATH / "match-transfermarkt-to-fbref" / "club_name_mapping_transfer_to_stat.pkl", "rb") as f:
        club_name_mapping = pickle.load(f)

    df_transfers = map_player_and_club_names_from_transfermarkt_to_fbref(
        df_transfers, df_name_matches, club_name_mapping
    )
    logging.info(
        "Mapping completed, was able to map %d transfers out of %d",
        len(df_transfers[df_transfers["player_name_mapped"].notna()]),
        len(df_transfers),
    )
    df_transfers_mapped = df_transfers.loc[df_transfers["player_name_mapped"].notna(), :]
    df_transfers_unmapped = df_transfers.loc[df_transfers["player_name_mapped"].isna(), :]

    df_transfers_mapped = add_club_domestic_competition_id(df_transfers_mapped, df_clubs)

    output_path = DATA_PATH / "processed" / f"transfers_relevant_mapped_{timestamp}.csv"
    unmapped_output_path = DATA_PATH / "processed" / f"transfers_relevant_unmapped_{timestamp}.csv"
    logging.info(f"Saving filtered transfers to {output_path}")
    logging.info(f"Saving unmapped transfers to {unmapped_output_path}")

    df_transfers_mapped.to_csv(output_path, index=False)
    df_transfers_unmapped.to_csv(unmapped_output_path, index=False)
    logging.info("Second filtering completed successfully")


if __name__ == "__main__":
    fire.Fire(get_transfers)
