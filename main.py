import os
import asyncio
from datetime import datetime
import random
from dotenv import load_dotenv
from loguru import logger

from api_client import FactCheckApiClient
from data_processor import DataProcessor
from storage import Storage
from contracts import Claim, CollectionStats

load_dotenv()
FACTCHECK_API_KEY = os.environ.get("FACTCHECK_API_KEY")
assert FACTCHECK_API_KEY, "FACTCHECK_API_KEY environment variable is required"

LOG_ROTATION_SIZE = "100 MB"
LOG_RETENTION_DAYS = "10 days"

logger.add(
    "factcheck_{time}.log",
    rotation=LOG_ROTATION_SIZE,
    retention=LOG_RETENTION_DAYS,
    level="DEBUG",
)


class FactCheckCollector:
    DEFAULT_PAGE_SIZE = 100
    DEFAULT_DISCOVERY_MAX_QUERIES = 200
    SAVE_BATCH_EVERY_N_PAGES = 10
    MAX_CONCURRENT_PUBLISHERS = 10
    MAX_CONCURRENT_DISCOVERY_QUERIES = 10

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

    async def _discover_query_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        query: str,
        publishers: set[str],
        index: int,
        total: int,
    ) -> int:
        """Execute a single discovery query with semaphore control."""
        async with semaphore:
            logger.info(f"[{index}/{total}] Discovery query: '{query}'")

            try:
                response = await self.api_client.fetch_page(
                    query=query, page_size=self.DEFAULT_PAGE_SIZE
                )
                query_publishers = self.processor.extract_publishers(
                    response.claims)
                publishers.update(query_publishers)

                logger.info(
                    f"Query '{query}' found {len(query_publishers)} publishers, "
                    f"total now: {len(publishers)}"
                )
                return len(query_publishers)

            except Exception as e:
                logger.error(f"Discovery query '{query}' failed: {e}")
                return 0

    async def discover_publishers(self, max_queries: int | None = None) -> set[str]:
        """Discover unique publisher sites using concurrent broad queries."""
        if max_queries is None:
            max_queries = self.DEFAULT_DISCOVERY_MAX_QUERIES

        discovery_queries = self._get_default_discovery_queries()
        random.shuffle(discovery_queries)

        num_queries = min(max_queries, len(discovery_queries))
        logger.info(
            f"Starting concurrent publisher discovery with {num_queries} queries "
            f"(max {self.MAX_CONCURRENT_DISCOVERY_QUERIES} at a time)"
        )

        publishers: set[str] = set()
        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_DISCOVERY_QUERIES)

        tasks = [
            self._discover_query_with_semaphore(
                semaphore, query, publishers, i, num_queries
            )
            for i, query in enumerate(discovery_queries[:num_queries], 1)
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

        logger.success(
            f"Discovery complete: {len(publishers)} publishers found")
        return publishers

    async def collect_from_publisher(
        self, publisher: str, output_file: str, page_size: int | None = None
    ) -> int:
        """Collect all claims from a single publisher with streaming saves."""
        if page_size is None:
            page_size = self.DEFAULT_PAGE_SIZE

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

                if page_count % self.SAVE_BATCH_EVERY_N_PAGES == 0:
                    flattened = self.processor.flatten_claims(batch_claims)
                    saved_count = await self.storage.save_claims(
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
                saved_count = await self.storage.save_claims(
                    flattened, output_file, append=True
                )
                total_count += saved_count

            logger.success(
                f"Publisher {publisher}: {total_count} claims collected")
            self.total_claims += total_count
            return total_count

        except Exception as e:
            logger.error(f"Publisher {publisher} failed: {e}")
            self.errors.append(
                {"publisher": publisher, "page": page_count, "error": str(e)}
            )
            if batch_claims:
                flattened = self.processor.flatten_claims(batch_claims)
                await self.storage.save_claims(flattened, output_file, append=True)
            return total_count

    async def _collect_publisher_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        publisher: str,
        output_file: str,
        page_size: int | None,
        index: int,
        total: int,
    ) -> tuple[str, int]:
        """Collect from a publisher with semaphore-based concurrency control."""
        async with semaphore:
            logger.info(f"[{index}/{total}] Processing {publisher}")
            count = await self.collect_from_publisher(publisher, output_file, page_size)
            return publisher, count

    async def collect_from_publishers(
        self, publishers: list[str], output_file: str, page_size: int | None = None
    ) -> dict[str, int]:
        """Collect claims from multiple publishers concurrently."""
        logger.info(
            f"Starting concurrent collection from {len(publishers)} publishers "
            f"(max {self.MAX_CONCURRENT_PUBLISHERS} at a time)"
        )

        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_PUBLISHERS)
        tasks = [
            self._collect_publisher_with_semaphore(
                semaphore, publisher, output_file, page_size, i, len(
                    publishers)
            )
            for i, publisher in enumerate(publishers, 1)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        stats = {}
        for result in results:
            if isinstance(result, BaseException):
                logger.error(f"Publisher task failed: {result}")
            else:
                publisher, count = result
                stats[publisher] = count

        return stats

    @staticmethod
    def _get_default_discovery_queries() -> list[str]:
        """
        Generate a comprehensive list of discovery queries.
        Covers years, topics, and common terms to maximize publisher discovery.
        """
        queries = []

        # Years (2015-2025)
        queries.extend([str(year) for year in range(2015, 2027)])

        # Politics & Elections
        queries.extend([
            "trump", "biden", "obama", "clinton", "election", "vote",
            "congress", "senate", "president", "democrat", "republican",
            "politician", "campaign", "ballot", "poll"
        ])

        # Health & Medicine
        queries.extend([
            "covid", "vaccine", "virus", "health", "doctor", "cure",
            "medicine", "drug", "hospital", "disease", "cancer", "flu",
            "mask", "lockdown", "pandemic", "WHO"
        ])

        # Science & Environment
        queries.extend([
            "climate", "warming", "science", "study", "research",
            "environment", "energy", "solar", "oil", "carbon", "NASA",
            "space", "earth", "weather", "disaster"
        ])

        # Technology
        queries.extend([
            "tech", "AI", "facebook", "google", "twitter", "apple",
            "amazon", "data", "hack", "cyber", "phone", "internet"
        ])

        # Economy & Business
        queries.extend([
            "economy", "tax", "job", "wage", "price", "inflation",
            "stock", "market", "bank", "dollar", "debt", "trade"
        ])

        # Social Issues
        queries.extend([
            "police", "crime", "gun", "law", "court", "judge", "rights",
            "immigration", "border", "war", "military", "terror",
            "protest", "violence", "racism", "gender"
        ])

        # Media & Misinformation
        queries.extend([
            "news", "media", "fake", "hoax", "false", "true", "fact",
            "claim", "rumor", "viral", "video", "photo", "quote",
            "misinformation", "debunk", "scam", "fraud"
        ])

        # International
        queries.extend([
            "China", "Russia", "Europe", "UK", "India", "Israel",
            "Ukraine", "Iran", "Mexico", "UN", "NATO", "world"
        ])

        # Short high-frequency terms
        queries.extend([
            "new", "says", "did", "will", "does", "can", "has", "was",
            "make", "said", "man", "woman", "people", "bill", "food",
            "water", "fire", "kill", "die", "dead", "safe", "ban"
        ])

        return queries


async def main():
    assert FACTCHECK_API_KEY is not None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    async with FactCheckApiClient(FACTCHECK_API_KEY) as api_client:
        storage = Storage()
        processor = DataProcessor()
        collector = FactCheckCollector(api_client, storage, processor)

        logger.info("=" * 70)
        logger.info("PHASE 1: Publisher Discovery")
        logger.info("=" * 70)

        publishers_file = storage.output_dir / "publishers.json"
        if publishers_file.exists():
            logger.info("Found existing publishers.json, loading...")
            loaded_data = storage.load_json("publishers.json")
            assert isinstance(loaded_data, list)
            publishers_list: list[str] = loaded_data
            logger.info(f"Loaded {len(publishers_list)} publishers from cache")
        else:
            publishers = await collector.discover_publishers()
            publishers_list = sorted(list(publishers))
            storage.save_json(publishers_list, "publishers.json")
            logger.info(
                f"Saved {len(publishers_list)} publishers to publishers.json")

        logger.info("=" * 70)
        logger.info("PHASE 2: Collecting Fact Checks")
        logger.info("=" * 70)

        raw_output = f"factchecks_raw_{timestamp}.jsonl"
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
