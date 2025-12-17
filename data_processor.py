from contracts import Claim, FlattenedClaim


class DataProcessor:
    @staticmethod
    def flatten_claim(claim: Claim) -> list[FlattenedClaim]:
        """Convert a claim with multiple reviews into multiple flattened rows."""
        base_data = {
            "claim_text": claim.text,
            "claimant": claim.claimant,
            "claim_date": claim.claim_date,
        }

        if not claim.claim_review:
            return [FlattenedClaim(**base_data)]

        flattened = []
        for review in claim.claim_review:
            review_data = {
                **base_data,
                "review_title": review.title,
                "review_url": review.url,
                "review_date": review.review_date,
                "textual_rating": review.textual_rating,
                "language_code": review.language_code,
                "publisher_name": review.publisher.name if review.publisher else None,
                "publisher_site": review.publisher.site if review.publisher else None,
            }
            flattened.append(FlattenedClaim(**review_data))

        return flattened

    @staticmethod
    def flatten_claims(claims: list[Claim]) -> list[FlattenedClaim]:
        """Flatten a list of claims into flattened rows."""
        flattened = []
        for claim in claims:
            flattened.extend(DataProcessor.flatten_claim(claim))
        return flattened

    @staticmethod
    def extract_publishers(claims: list[Claim]) -> set[str]:
        """Extract unique publisher sites from claims."""
        publishers = set()
        for claim in claims:
            for review in claim.claim_review:
                if review.publisher and review.publisher.site:
                    publishers.add(review.publisher.site)
        return publishers
