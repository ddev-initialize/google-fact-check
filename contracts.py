from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field


class Publisher(BaseModel):
    name: Optional[str] = None
    site: Optional[str] = None


class ClaimReview(BaseModel):
    url: Optional[str] = None
    title: Optional[str] = None
    review_date: Optional[str] = Field(None, alias="reviewDate")
    textual_rating: Optional[str] = Field(None, alias="textualRating")
    language_code: Optional[str] = Field(None, alias="languageCode")
    publisher: Optional[Publisher] = None

    class Config:
        populate_by_name = True


class Claim(BaseModel):
    text: Optional[str] = None
    claimant: Optional[str] = None
    claim_date: Optional[str] = Field(None, alias="claimDate")
    claim_review: list[ClaimReview] = Field(default_factory=list, alias="claimReview")

    class Config:
        populate_by_name = True


class FactCheckApiResponse(BaseModel):
    claims: list[Claim] = Field(default_factory=list)
    next_page_token: Optional[str] = Field(None, alias="nextPageToken")

    class Config:
        populate_by_name = True


class FlattenedClaim(BaseModel):
    claim_text: Optional[str] = None
    claimant: Optional[str] = None
    claim_date: Optional[str] = None
    review_title: Optional[str] = None
    review_url: Optional[str] = None
    review_date: Optional[str] = None
    textual_rating: Optional[str] = None
    language_code: Optional[str] = None
    publisher_name: Optional[str] = None
    publisher_site: Optional[str] = None


class CollectionStats(BaseModel):
    publishers_discovered: int
    total_requests: int
    total_claims_collected: int
    publisher_stats: dict[str, int]
    errors: list[dict[str, Any]]
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
