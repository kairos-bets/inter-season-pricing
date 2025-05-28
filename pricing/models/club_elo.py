from pydantic import BaseModel


class ClubElo(BaseModel):
    """Pydantic model for club ELO data"""

    rank: int
    club: str
    country: str  # ENG
    level: int
    elo: float
    from_date: str  # YYYY-MM-DD
    to_date: str  # YYYY-MM-DD
