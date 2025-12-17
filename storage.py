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
        """Save flattened claims to Parquet file."""
        if not claims:
            return 0

        data = [claim.model_dump() for claim in claims]
        df = pl.DataFrame(data)
        filepath = self.output_dir / filename

        if append and filepath.exists():
            existing_df = pl.read_parquet(filepath)
            df = pl.concat([existing_df, df])

        df.write_parquet(filepath, compression="snappy")
        logger.debug(f"Saved {len(claims)} claims to {filename}")

        return len(claims)

    def deduplicate_file(self, input_file: str, output_file: str) -> pl.DataFrame:
        """Deduplicate dataset by review_url."""
        filepath = self.output_dir / input_file
        df = pl.read_parquet(filepath)
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
