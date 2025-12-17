import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger

from api_client import FactCheckApiClient
from data_processor import DataProcessor
from storage import Storage
from contracts import Claim, CollectionStats

load_dotenv()
FACTCHECK_API_KEY = os.environ.get("FACTCHECK_API_KEY")
assert FACTCHECK_API_KEY, "FACTCHECK_API_KEY environment variable is required"

logger.add(
    "factcheck_{time}.log",
    rotation="100 MB",
)


class FactCheckCollector:
    DISCOVERY_QUERIES = [
        "covid",
        "election",
        "climate",
        "vaccine",
        "trump",
        "biden",
        "health",
        "politics",
        "science",
        "news",
        "fact",
        "claim",
        "false",
        "true",
        "misinformation",
        "hoax",
        "debunk",
        "2024",
        "2023",
        "2022",
        "government",
    ]

    def __init__(
        self,
        api_client: FactCheckApiClient,
        storage: Storage,
        processor: DataProcessor,
    ):
        self.api_client = api_client
        self.storage = storage
        self.processor = processor
        self.total_claims = 0
        self.errors: list[dict[str, str | int]] = []

    async def discover_publishers(self, max_queries: int = 20) -> set[str]:
        """Discover unique publisher sites using broad queries."""
        logger.info(f"Starting publisher discovery with {max_queries} queries")
        publishers = set()

        for i, query in enumerate(self.DISCOVERY_QUERIES[:max_queries], 1):
            logger.info(f"[{i}/{max_queries}] Discovery query: '{query}'")

            try:
                response = await self.api_client.fetch_page(query=query, page_size=100)
                query_publishers = self.processor.extract_publishers(
                    response.claims)
                publishers.update(query_publishers)
                logger.info(
                    f"Found {len(publishers)} unique publishers so far")

            except Exception as e:
                logger.error(f"Discovery query '{query}' failed: {e}")
                continue

        logger.success(
            f"Discovery complete: {len(publishers)} publishers found")
        return publishers

    async def collect_from_publisher(
        self, publisher: str, output_file: str, page_size: int = 100
    ) -> int:
        """Collect all claims from a single publisher with streaming saves."""
        logger.info(f"Collecting from publisher: {publisher}")
        batch_claims: list[Claim] = []
        page_count = 0
        total_count = 0

        try:
            responses = await self.api_client.fetch_all_pages(
                publisher_filter=publisher, page_size=page_size
            )

            for response in responses:
                batch_claims.extend(response.claims)
                page_count += 1

                if page_count % 10 == 0:
                    flattened = self.processor.flatten_claims(batch_claims)
                    saved_count = self.storage.save_claims(
                        flattened, output_file, append=True
                    )
                    total_count += saved_count
                    logger.debug(
                        f"Page {page_count}: Saved {len(batch_claims)} claims "
                        f"(Total: {total_count})"
                    )
                    batch_claims = []

            if batch_claims:
                flattened = self.processor.flatten_claims(batch_claims)
                saved_count = self.storage.save_claims(
                    flattened, output_file, append=True
                )
                total_count += saved_count

            logger.success(
                f"Publisher {publisher}: {total_count} claims collected")
            return total_count

        except Exception as e:
            logger.error(f"Publisher {publisher} failed: {e}")
            self.errors.append(
                {"publisher": publisher, "page": page_count, "error": str(e)}
            )
            if batch_claims:
                flattened = self.processor.flatten_claims(batch_claims)
                self.storage.save_claims(flattened, output_file, append=True)
            return total_count

    async def collect_from_publishers(
        self, publishers: list[str], output_file: str, page_size: int = 100
    ) -> dict[str, int]:
        """Collect claims from multiple publishers."""
        logger.info(f"Starting collection from {len(publishers)} publishers")
        stats = {}

        for i, publisher in enumerate(publishers, 1):
            logger.info(f"[{i}/{len(publishers)}] Processing {publisher}")
            count = await self.collect_from_publisher(publisher, output_file, page_size)
            stats[publisher] = count
            self.total_claims += count

        return stats


async def main():
    assert FACTCHECK_API_KEY is not None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    async with FactCheckApiClient(
        FACTCHECK_API_KEY, max_concurrent=10, requests_per_100s=1000
    ) as api_client:
        storage = Storage()
        processor = DataProcessor()
        collector = FactCheckCollector(api_client, storage, processor)

        logger.info("=" * 70)
        logger.info("PHASE 1: Publisher Discovery")
        logger.info("=" * 70)

        publishers = await collector.discover_publishers(max_queries=20)
        publishers_list = sorted(list(publishers))

        storage.save_json(publishers_list, "publishers.json")
        logger.info(
            f"Saved {len(publishers_list)} publishers to publishers.json")

        logger.info("=" * 70)
        logger.info("PHASE 2: Collecting Fact Checks")
        logger.info("=" * 70)

        raw_output = f"factchecks_raw_{timestamp}.parquet"
        publisher_stats = await collector.collect_from_publishers(
            publishers_list, raw_output
        )

        logger.info("=" * 70)
        logger.info("PHASE 3: Deduplication")
        logger.info("=" * 70)

        final_output = f"factchecks_final_{timestamp}.parquet"
        storage.deduplicate_file(raw_output, final_output)

        stats = CollectionStats(
            publishers_discovered=len(publishers_list),
            total_requests=api_client.total_requests,
            total_claims_collected=collector.total_claims,
            publisher_stats=publisher_stats,
            errors=collector.errors,
        )

        stats_file = f"collection_stats_{timestamp}.json"
        storage.save_json(stats.model_dump(), stats_file)

        logger.success("=" * 70)
        logger.success("COLLECTION COMPLETE")
        logger.success("=" * 70)
        logger.success(f"Publishers discovered: {len(publishers_list)}")
        logger.success(f"API requests made: {api_client.total_requests:,}")
        logger.success(f"Total rows collected: {collector.total_claims:,}")
        logger.success(f"Errors encountered: {len(collector.errors)}")


if __name__ == "__main__":
    asyncio.run(main())
