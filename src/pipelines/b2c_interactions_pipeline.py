from collections import defaultdict
from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class B2CInteractionsPipeline:
    """Aggregate B2C recipe/product interactions into user→item edges."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("b2c_interactions_pipeline")

    # ===================== DATA LOADERS =====================
    def load_user(self, conn, user_id: str) -> Optional[Dict]:
        sql = """
        SELECT id, email, full_name, updated_at
        FROM b2c_customers
        WHERE id = %s;
        """
        return pg.fetch_one(conn, sql, (user_id,))

    def load_recipe_history(self, conn, user_id: str) -> List[Dict]:
        sql = """
        SELECT recipe_id, event_type, event_at
        FROM recipe_history
        WHERE user_id = %s;
        """
        return pg.fetch_all(conn, sql, (user_id,))

    def load_saved_recipes(self, conn, user_id: str) -> List[Dict]:
        sql = """
        SELECT recipe_id, saved_at
        FROM saved_recipes
        WHERE user_id = %s;
        """
        return pg.fetch_all(conn, sql, (user_id,))

    def load_recipe_ratings(self, conn, user_id: str) -> List[Dict]:
        sql = """
        SELECT recipe_id, rating, created_at
        FROM recipe_ratings
        WHERE b2c_customer_id = %s;
        """
        return pg.fetch_all(conn, sql, (user_id,))

    def load_product_interactions(self, conn, user_id: str) -> List[Dict]:
        sql = """
        SELECT product_id, interaction_type, rating, quantity, price_paid, interaction_timestamp
        FROM customer_product_interactions
        WHERE b2c_customer_id = %s;
        """
        return pg.fetch_all(conn, sql, (user_id,))

    # ===================== AGGREGATION =====================
    def aggregate_recipes(self, history: List[Dict], saved: List[Dict], ratings: List[Dict]) -> List[Dict]:
        agg = defaultdict(lambda: {
            "recipe_id": None,
            "views_count": 0,
            "last_view_at": None,
            "cooks_count": 0,
            "last_cook_at": None,
            "saved": False,
            "first_saved_at": None,
            "rating": None,
            "last_rating_at": None,
        })
        for row in history:
            rid = row["recipe_id"]
            entry = agg[rid]
            entry["recipe_id"] = rid
            if row["event_type"] == "viewed":
                entry["views_count"] += 1
                entry["last_view_at"] = max(entry["last_view_at"], row["event_at"]) if entry["last_view_at"] else row["event_at"]
            elif row["event_type"] == "cooked":
                entry["cooks_count"] += 1
                entry["last_cook_at"] = max(entry["last_cook_at"], row["event_at"]) if entry["last_cook_at"] else row["event_at"]

        for row in saved:
            rid = row["recipe_id"]
            entry = agg[rid]
            entry["recipe_id"] = rid
            entry["saved"] = True
            entry["first_saved_at"] = min(entry["first_saved_at"], row["saved_at"]) if entry["first_saved_at"] else row["saved_at"]

        for row in ratings:
            rid = row["recipe_id"]
            entry = agg[rid]
            entry["recipe_id"] = rid
            entry["rating"] = row["rating"]
            entry["last_rating_at"] = row["created_at"]

        return list(agg.values())

    def aggregate_products(self, interactions: List[Dict]) -> List[Dict]:
        agg = defaultdict(lambda: {
            "product_id": None,
            "views_count": 0,
            "last_view_at": None,
            "purchases_count": 0,
            "last_purchase_at": None,
            "saved": False,
            "rating": None,
            "last_rating_at": None,
            "quantity_total": 0,
            "price_total": 0,
        })
        for row in interactions:
            pid = row["product_id"]
            entry = agg[pid]
            entry["product_id"] = pid
            itype = row["interaction_type"]
            ts = row["interaction_timestamp"]
            if itype == "viewed":
                entry["views_count"] += 1
                entry["last_view_at"] = max(entry["last_view_at"], ts) if entry["last_view_at"] else ts
            elif itype == "purchased":
                entry["purchases_count"] += 1
                entry["last_purchase_at"] = max(entry["last_purchase_at"], ts) if entry["last_purchase_at"] else ts
                entry["quantity_total"] += row.get("quantity") or 0
                entry["price_total"] += row.get("price_paid") or 0
            elif itype == "saved":
                entry["saved"] = True
            if row.get("rating"):
                entry["rating"] = row["rating"]
                entry["last_rating_at"] = ts
        return list(agg.values())

    # ===================== CYPHER =====================
    def _upsert_cypher(self) -> str:
        return """
        MERGE (u:B2CCustomer {id: $user.id})
        SET u.email = $user.email,
            u.full_name = $user.full_name,
            u.updated_at = datetime($user.updated_at)

        // Clear old recipe edges
        WITH u
        OPTIONAL MATCH (u)-[r:VIEWED|COOKED|SAVED|RATED]->(:Recipe)
        DELETE r;

        // Rebuild recipe interactions
        WITH u, $recipes AS recipes
        UNWIND recipes AS i
          MERGE (r:Recipe {id: i.recipe_id})
          FOREACH (_ IN CASE WHEN i.views_count > 0 THEN [1] ELSE [] END |
            MERGE (u)-[v:VIEWED]->(r)
            SET v.count = i.views_count,
                v.last_at = datetime(i.last_view_at)
          )
          FOREACH (_ IN CASE WHEN i.cooks_count > 0 THEN [1] ELSE [] END |
            MERGE (u)-[c:COOKED]->(r)
            SET c.count = i.cooks_count,
                c.last_at = datetime(i.last_cook_at)
          )
          FOREACH (_ IN CASE WHEN i.saved THEN [1] ELSE [] END |
            MERGE (u)-[s:SAVED]->(r)
            SET s.first_saved_at = datetime(i.first_saved_at)
          )
          FOREACH (_ IN CASE WHEN i.rating IS NULL THEN [] ELSE [1] END |
            MERGE (u)-[rr:RATED]->(r)
            SET rr.rating = i.rating,
                rr.last_at = datetime(i.last_rating_at)
          );

        // Clear old product edges
        WITH u
        OPTIONAL MATCH (u)-[pr:VIEWED_PRODUCT|PURCHASED|SAVED_PRODUCT|RATED_PRODUCT]->(:Product)
        DELETE pr;

        // Rebuild product interactions
        WITH u, $products AS products
        UNWIND products AS p
          MERGE (prod:Product {id: p.product_id})
          FOREACH (_ IN CASE WHEN p.views_count > 0 THEN [1] ELSE [] END |
            MERGE (u)-[vp:VIEWED_PRODUCT]->(prod)
            SET vp.count = p.views_count,
                vp.last_at = datetime(p.last_view_at)
          )
          FOREACH (_ IN CASE WHEN p.purchases_count > 0 THEN [1] ELSE [] END |
            MERGE (u)-[pu:PURCHASED]->(prod)
            SET pu.count = p.purchases_count,
                pu.last_at = datetime(p.last_purchase_at),
                pu.quantity_total = p.quantity_total,
                pu.price_total = p.price_total
          )
          FOREACH (_ IN CASE WHEN p.saved THEN [1] ELSE [] END |
            MERGE (u)-[sp:SAVED_PRODUCT]->(prod)
          )
          FOREACH (_ IN CASE WHEN p.rating IS NULL THEN [] ELSE [1] END |
            MERGE (u)-[rp:RATED_PRODUCT]->(prod)
            SET rp.rating = p.rating,
                rp.last_at = datetime(p.last_rating_at)
          );
        """

    def _delete_cypher(self) -> str:
        return "MATCH (u:B2CCustomer {id: $id}) DETACH DELETE u;"

    # ===================== OPERATIONS =====================
    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            user = self.load_user(conn, event.aggregate_id)

        if user is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting B2C user interactions", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning("B2C user missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        with self.pg_pool.connection() as conn:
            history = self.load_recipe_history(conn, event.aggregate_id)
            saved = self.load_saved_recipes(conn, event.aggregate_id)
            ratings = self.load_recipe_ratings(conn, event.aggregate_id)
            prod_interactions = self.load_product_interactions(conn, event.aggregate_id)

        recipe_agg = self.aggregate_recipes(history, saved, ratings)
        product_agg = self.aggregate_products(prod_interactions)

        params = {"user": user, "recipes": recipe_agg, "products": product_agg}
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info(
            "Upserted B2C interactions",
            extra={"id": event.aggregate_id, "recipes": len(recipe_agg), "products": len(product_agg)},
        )
