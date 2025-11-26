import time
from typing import List

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.queue.outbox import fetch_pending_events, mark_failed, mark_processed
from src.adapters.supabase.db import PostgresPool
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.pipelines.b2c_interactions_pipeline import B2CInteractionsPipeline
from src.pipelines.b2b_interactions_pipeline import B2BInteractionsPipeline
from src.utils.logging import configure_logging


TABLES = [
    "saved_recipes",
    "recipe_history",
    "recipe_ratings",
    "customer_product_interactions",
    # B2B-side tables (assumed names; adjust if different)
    "vendor_user_actions",
    "match_feedback",
]

AGG_TYPES = ["b2c_interaction", "b2b_interaction"]


def process_batch(b2c_pipeline, b2b_pipeline, events: List[OutboxEvent], pg_pool: PostgresPool, log):
    for event in events:
        try:
            agg = event.aggregate_type
            if agg == "b2c_interaction":
                b2c_pipeline.handle_event(event)
            elif agg == "b2b_interaction":
                b2b_pipeline.handle_event(event)
            else:
                log.warning("Unhandled aggregate type", extra={"aggregate_type": agg, "event_id": event.id})
                continue

            with pg_pool.connection() as conn:
                mark_processed(conn, event.id)
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed processing interaction event", extra={"event_id": event.id, "aggregate_id": event.aggregate_id})
            with pg_pool.connection() as conn:
                mark_failed(conn, event.id, str(exc))


def main():
    settings = Settings()
    log = configure_logging("feedback_interactions_worker")
    log.info("Starting feedback/interactions worker", extra={"pipeline": settings.pipeline_name})

    pg_pool = PostgresPool(settings.supabase_dsn)
    neo4j = Neo4jClient(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)

    b2c_pipeline = B2CInteractionsPipeline(settings, pg_pool, neo4j)
    b2b_pipeline = B2BInteractionsPipeline(settings, pg_pool, neo4j)

    try:
        while True:
            with pg_pool.connection() as conn:
                conn.autocommit = False
                events = fetch_pending_events(
                    conn,
                    settings.batch_size,
                    settings.max_attempts,
                    table_names=TABLES,
                    aggregate_types=AGG_TYPES,
                )
                conn.commit()

            if not events:
                time.sleep(settings.poll_interval_seconds)
                continue

            process_batch(b2c_pipeline, b2b_pipeline, events, pg_pool, log)
    finally:
        neo4j.close()
        pg_pool.close()


if __name__ == "__main__":
    main()
