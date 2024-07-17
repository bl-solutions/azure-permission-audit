from neo4j import Session
from pydantic import BaseModel


class Subscription(BaseModel):
    """Represent an Azure subscription"""

    identifier: str
    name: str

    def merge_record(self, session: Session) -> None:
        """Record the subscription to database"""
        query = "MERGE (:SUBSCRIPTION {id: '%s', name: '%s'})" % (
            self.identifier,
            self.name,
        )
        with session.begin_transaction() as tx:
            tx.run(query)
            tx.commit()
