from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, field_validator


class PlayerMatchLogs(BaseModel):
    """Pydantic model for player match statistics"""

    date: Optional[str] = None
    dayofweek: Optional[str] = None
    round: Optional[str] = None
    venue: Optional[str] = None
    result: Optional[str] = None
    team: Optional[str] = None
    opponent: Optional[str] = None
    game_started: Optional[str] = None
    position: Optional[str] = None
    minutes: Optional[int] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    pens_made: Optional[int] = None
    pens_att: Optional[int] = None
    shots: Optional[int] = None
    shots_on_target: Optional[int] = None
    cards_yellow: Optional[int] = None
    cards_red: Optional[int] = None
    touches: Optional[int] = None
    tackles: Optional[int] = None
    interceptions: Optional[int] = None
    blocks: Optional[int] = None
    xg: Optional[float] = None
    npxg: Optional[float] = None
    xg_assist: Optional[float] = None
    sca: Optional[int] = None
    gca: Optional[int] = None
    passes_completed: Optional[int] = None
    passes: Optional[int] = None
    passes_pct: Optional[float] = None
    progressive_passes: Optional[int] = None
    carries: Optional[int] = None
    progressive_carries: Optional[int] = None
    take_ons: Optional[int] = None
    take_ons_won: Optional[int] = None

    player_name: str
    player_id: str
    stat_type: str

    # Added by the data loader
    season: Optional[str] = None
    league: Optional[str] = None

    @field_validator("*", mode="before")
    def empty_str_to_none(cls: Any, v: Any) -> Optional[Any]:
        if v == "":
            return None
        return v


class PostTransferPlayerMatchLogs(PlayerMatchLogs):
    """Extended model for post-transfer match logs with additional transfer information"""

    transfer_id: str
    transfer_date: datetime
    from_club: str
    to_club: str
    match_number_after_transfer: int
    days_since_transfer: Optional[int] = None


class MatchElo(BaseModel):
    """Pydantic model for match ELO data"""

    date: Optional[str] = None
    season: Optional[str] = None
    home_team: str
    away_team: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    home_elo_before: Optional[float] = None
    away_elo_before: Optional[float] = None
    home_elo_after: Optional[float] = None
    away_elo_after: Optional[float] = None
    home_elo_change: Optional[float] = None
    away_elo_change: Optional[float] = None
    home_win_prob: Optional[float] = None
    home_power_before: Optional[float] = None
    away_power_before: Optional[float] = None
    home_power_after: Optional[float] = None
    away_power_after: Optional[float] = None

    @field_validator("*", mode="before")
    def empty_str_to_none(cls: Any, v: Any) -> Optional[Any]:
        if v == "":
            return None
        return v
