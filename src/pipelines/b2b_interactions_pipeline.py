from collections import defaultdict
from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class B2BInteractionsPipeline:
    """Aggregate B2B vendor user interactions into user→product/match edges."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("b2b_interactions_pipeline")

    # ===================== DATA LOADERS =====================
    def load_vendor_user(self, conn, user_id: str) -> Optional[Dict]:
        sql = """
        SELECT vu.*, v.id AS vendor_id, v.name AS vendor_name
        FROM vendor_users vu
        JOIN vendors v ON v.id = vu.vendor_id
        WHERE vu.id = %s;
        """
        return pg.fetch_one(conn, sql, (user_id,))

    def load_product_actions(self, conn, user_id: str) -> List[Dict]:
        sql = """
        SELECT product_id, action_type, created_at
        FROM vendor_user_actions
        WHERE vendor_user_id = %s AND product_id IS NOT NULL;
        """
        return pg.fetch_all(conn, sql, (user_id,))

    def load_match_feedback(self, conn, user_id: str) -> List[Dict]:
        sql = """
        SELECT source_product_id, target_product_id, feedback_type, reason, created_at
        FROM match_feedback
        WHERE vendor_user_id = %s;
        """
        return pg.fetch_all(conn, sql, (user_id,))

    # ===================== AGGREGATION =====================
    def aggregate_products(self, actions: List[Dict]) -> List[Dict]:
        agg = defaultdict(lambda: {"product_id": None, "views_count": 0, "last_view_at": None})
        for row in actions:
            pid = row["product_id"]
            entry = agg[pid]
            entry["product_id"] = pid
            if row["action_type"] == "view_product":
                entry["views_count"] += 1
                ts = row["created_at"]
                entry["last_view_at"] = max(entry["last_view_at"], ts) if entry["last_view_at"] else ts
        return list(agg.values())

    def aggregate_matches(self, feedback: List[Dict]) -> List[Dict]:
        agg = defaultdict(lambda: {
            "source_product_id": None,
            "target_product_id": None,
            "approved": False,
            "rejected": False,
            "reason": None,
            "last_feedback_at": None,
        })
        for row in feedback:
            key = (row["source_product_id"], row["target_product_id"])
            entry = agg[key]
            entry["source_product_id"] = row["source_product_id"]
            entry["target_product_id"] = row["target_product_id"]
            ts = row["created_at"]
            entry["last_feedback_at"] = max(entry["last_feedback_at"], ts) if entry["last_feedback_at"] else ts
            if row["feedback_type"] == "approved":
                entry["approved"] = True
            elif row["feedback_type"] == "rejected":
                entry["rejected"] = True
                entry["reason"] = row["reason"]
        return list(agg.values())

    # ===================== CYPHER =====================
    def _upsert_cypher(self) -> str:
        return """
        MERGE (u:VendorUser {id: $user.id})
        SET u.email = $user.email,
            u.role = $user.role,
            u.updated_at = datetime($user.updated_at)

        MERGE (v:Vendor {id: $user.vendor_id})
        SET v.name = $user.vendor_name

        MERGE (u)-[:BELONGS_TO_VENDOR]->(v)

        // Clear old product edges
        WITH u
        OPTIONAL MATCH (u)-[r:VIEWED_PRODUCT]->(:Product)
        DELETE r;

        // Rebuild product interactions
        WITH u, $products AS products
        UNWIND products AS p
          MERGE (prod:Product {id: p.product_id})
          FOREACH (_ IN CASE WHEN p.views_count > 0 THEN [1] ELSE [] END |
            MERGE (u)-[vp:VIEWED_PRODUCT]->(prod)
            SET vp.count = p.views_count,
                vp.last_at = datetime(p.last_view_at)
          );

        // Clear old match feedback edges
        WITH u
        OPTIONAL MATCH (u)-[r:APPROVED_MATCH|REJECTED_MATCH]->(:Product)
        DELETE r;

        // Rebuild match feedback
        WITH u, $matches AS matches
        UNWIND matches AS m
          MERGE (src:Product {id: m.source_product_id})
          MERGE (tgt:Product {id: m.target_product_id})
          FOREACH (_ IN CASE WHEN m.approved THEN [1] ELSE [] END |
            MERGE (u)-[am:APPROVED_MATCH]->(tgt)
            SET am.source_product_id = m.source_product_id,
                am.last_at = datetime(m.last_feedback_at)
          )
          FOREACH (_ IN CASE WHEN m.rejected THEN [1] ELSE [] END |
            MERGE (u)-[rm:REJECTED_MATCH]->(tgt)
            SET rm.source_product_id = m.source_product_id,
                rm.reason = m.reason,
                rm.last_at = datetime(m.last_feedback_at)
          );
        """

    def _delete_cypher(self) -> str:
        return "MATCH (u:VendorUser {id: $id}) DETACH DELETE u;"

    # ===================== OPERATIONS =====================
    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            user = self.load_vendor_user(conn, event.aggregate_id)

        if user is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting vendor user interactions", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning("Vendor user missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        with self.pg_pool.connection() as conn:
            product_actions = self.load_product_actions(conn, event.aggregate_id)
            match_feedback = self.load_match_feedback(conn, event.aggregate_id)

        product_agg = self.aggregate_products(product_actions)
        match_agg = self.aggregate_matches(match_feedback)

        params = {"user": user, "products": product_agg, "matches": match_agg}
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info(
            "Upserted B2B interactions",
            extra={"id": event.aggregate_id, "products": len(product_agg), "matches": len(match_agg)},
        )
