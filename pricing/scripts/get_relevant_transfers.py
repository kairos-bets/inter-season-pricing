"""
This script filters the relevant transfers from the transfermarkt data based on various criteria
"""

import logging
from datetime import datetime
from pathlib import Path

import fire

from pricing.format.transfer import get_relevant_transfers, load_clubs, load_transfers

DATA_PATH = Path(__file__).parent.parent.parent / "data"
TRANSFERMARKT_DATA_PATH = DATA_PATH / "transfermarkt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main() -> None:
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
    logging.info("Processing completed successfully")


if __name__ == "__main__":
    fire.Fire(main)
