from typing import Any, Dict, Iterable

from neo4j import GraphDatabase, Transaction


class Neo4jClient:
    """Thin wrapper around the Neo4j driver to keep a consistent API."""

    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self._driver.close()

    def write(self, cypher: str, parameters: Dict[str, Any]) -> None:
        with self._driver.session() as session:
            session.execute_write(lambda tx: tx.run(cypher, **parameters))

    def write_transaction(self, fn, *args, **kwargs):
        with self._driver.session() as session:
            return session.execute_write(fn, *args, **kwargs)

    def read(self, cypher: str, parameters: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        with self._driver.session() as session:
            result = session.run(cypher, **parameters)
            return [record.data() for record in result]
