# Pipeline: feedback-interactions

Scope: Ingest B2C/B2B feedback & interactions from Supabase Gold v3 into Neo4j v3 via a Python worker. This repo is self-contained (no shared code).

Supabase source tables: saved_recipes, recipe_history, recipe_ratings, customer_product_interactions, vendor_user_actions, match_feedback (adjust if table names differ)
Neo4j labels touched: B2CCustomer, VendorUser, Recipe, Product
Neo4j relationships touched: VIEWED, COOKED, SAVED, RATED (recipes); VIEWED_PRODUCT, PURCHASED, SAVED_PRODUCT, RATED_PRODUCT (products); APPROVED_MATCH, REJECTED_MATCH (B2B matches)

How it works
- Outbox-driven: worker polls `outbox_events` filtered to interaction tables/aggregate types (b2c_interaction, b2b_interaction), locks with `SKIP LOCKED`, routes per aggregate.
- B2C pipeline: aggregates a user’s recipe history, saved recipes, ratings, and product interactions into per-user edges; rebuilds all edges per user; DELETE events detach-delete user interactions when the user is missing.
- B2B pipeline: aggregates vendor user product views and match feedback into edges; rebuilds per user; DELETE events detach-delete vendor user interactions when the user is missing.

Run
- Install deps: `pip install -r requirements.txt`
- Configure env: copy `.env.example` → `.env` and fill Postgres/Neo4j credentials.
- Start worker: `python -m src.workers.runner`

Folders
- docs/: domain notes, Cypher patterns, event routing
- src/: config, adapters (supabase, neo4j, queue), domain models/services, pipelines (aggregate upserts), workers (runners), utils
- tests/: placeholder for unit/integration tests
- ops/: ops templates (docker/env/sample cron jobs)
