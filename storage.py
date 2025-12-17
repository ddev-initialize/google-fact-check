from pathlib import Path
import json
import polars as pl
from loguru import logger

from contracts import FlattenedClaim


class Storage:
    def __init__(self, output_dir: str = "factcheck_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def save_claims(
        self, claims: list[FlattenedClaim], filename: str, append: bool = True
    ) -> int:
        """Save flattened claims to JSONL file."""
        if not claims:
            return 0

        filepath = self.output_dir / filename
        mode = "a" if (append and filepath.exists()) else "w"

        with open(filepath, mode) as f:
            for claim in claims:
                f.write(json.dumps(claim.model_dump()) + "\n")

        logger.debug(f"Saved {len(claims)} claims to {filename}")
        return len(claims)

    def deduplicate_file(self, input_file: str, output_file: str) -> pl.DataFrame:
        """Deduplicate dataset by review_url."""
        filepath = self.output_dir / input_file
        df = pl.read_ndjson(filepath)
        original_count = len(df)

        df_deduped = df.unique(subset=["review_url"], keep="first")
        final_count = len(df_deduped)
        duplicates = original_count - final_count

        output_path = self.output_dir / output_file
        df_deduped.write_parquet(output_path, compression="snappy")

        logger.info(
            f"Deduplicated {input_file}: {original_count:,} → {final_count:,} "
            f"({duplicates:,} duplicates removed)"
        )

        return df_deduped

    def save_json(self, data: dict | list, filename: str):
        """Save data as JSON."""
        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        logger.debug(f"Saved JSON to {filename}")

    def load_json(self, filename: str) -> dict | list:
        """Load data from JSON."""
        filepath = self.output_dir / filename
        with open(filepath, "r") as f:
            return json.load(f)
