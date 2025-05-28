from pathlib import Path

import pandas as pd

# Setup paths
DATA_PATH = Path(__file__).parent.parent.parent / "data"

# Load the players data
df_players = pd.read_csv(DATA_PATH / "processed" / "players_to_scrape_20250423.csv", sep=",")

# Create separate dataframes for from_clubs and to_clubs
from_clubs = df_players[
    [
        "from_club_name_transfermarkt",
        "from_club",
        "from_club_domestic_competition_id",
        "from_club_domestic_competition_name",
    ]
].rename(
    columns={
        "from_club_name_transfermarkt": "club_name_transfermarkt",
        "from_club": "club_name_fbref",
        "from_club_domestic_competition_id": "club_domestic_competition_id_transfermarkt",
        "from_club_domestic_competition_name": "club_domestic_competition_name_fbref",
    }
)

to_clubs = df_players[
    ["to_club_name_transfermarkt", "to_club", "to_club_domestic_competition_id", "to_club_domestic_competition_name"]
].rename(
    columns={
        "to_club_name_transfermarkt": "club_name_transfermarkt",
        "to_club": "club_name_fbref",
        "to_club_domestic_competition_id": "club_domestic_competition_id_transfermarkt",
        "to_club_domestic_competition_name": "club_domestic_competition_name_fbref",
    }
)

# Combine both dataframes and get unique clubs
unique_clubs = pd.concat([from_clubs, to_clubs], ignore_index=True).drop_duplicates()

# Sort by club name for better readability
unique_clubs = unique_clubs.sort_values("club_name_transfermarkt").reset_index(drop=True)

print(f"Total unique clubs: {len(unique_clubs)}")
print("\nFirst 10 unique clubs:")
print(unique_clubs.head(10))

# Save the unique clubs to a CSV file
output_file = DATA_PATH / "processed" / "unique_clubs_mapping.csv"
unique_clubs.to_csv(output_file, index=False)
print(f"\nUnique clubs saved to: {output_file}")

# Display some stats
print("\nDataset statistics:")
print(f"- Total unique clubs: {len(unique_clubs)}")
print(f"- Unique transfermarkt names: {unique_clubs['club_name_transfermarkt'].nunique()}")
print(f"- Unique fbref names: {unique_clubs['club_name_fbref'].nunique()}")
print(f"- Unique competition IDs: {unique_clubs['club_domestic_competition_id_transfermarkt'].nunique()}")
