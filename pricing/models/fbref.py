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
