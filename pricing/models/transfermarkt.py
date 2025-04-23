from datetime import date
from typing import Any, Optional

from pydantic import BaseModel, field_validator


def empty_str_to_none(v: Any) -> Any:
    if v == "":
        return None
    return v


class TransfermarktTransfer(BaseModel):
    player_id: int
    transfer_date: date
    transfer_season: str
    from_club_id: int
    to_club_id: int
    from_club_name: str
    to_club_name: str
    transfer_fee: Optional[float] = None
    market_value_in_eur: Optional[float] = None
    player_name: str

    @field_validator("transfer_fee", "market_value_in_eur", mode="before")
    @classmethod
    def handle_empty_transfer_fee(cls, v: Any) -> Any:
        return empty_str_to_none(v)


class TransfermarktTransferMapped(TransfermarktTransfer):
    player_name_mapped: str
    from_club_name_mapped: str
    to_club_name_mapped: str
    from_club_domestic_competition_id: str
    to_club_domestic_competition_id: str


class TransfermarktClub(BaseModel):
    club_id: int
    club_code: str
    name: str
    domestic_competition_id: str
    total_market_value: Optional[float] = None
    squad_size: Optional[int] = None
    average_age: Optional[float] = None
    foreigners_number: Optional[int] = None
    foreigners_percentage: Optional[float] = None
    national_team_players: Optional[int] = None
    stadium_name: Optional[str] = None
    stadium_seats: Optional[int] = None
    net_transfer_record: Optional[str] = None
    coach_name: Optional[str] = None
    last_season: int
    filename: str
    url: str

    @field_validator(
        "total_market_value",
        "average_age",
        "foreigners_percentage",
        "squad_size",
        "foreigners_number",
        "national_team_players",
        "stadium_seats",
        mode="before",
    )
    @classmethod
    def handle_empty_numeric(cls, v: Any) -> Any:
        return empty_str_to_none(v)
