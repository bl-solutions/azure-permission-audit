from azure.identity import DefaultAzureCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from neo4j import Session
from pydantic import BaseModel

from models import Assignment, PrincipalType


class Subscription(BaseModel):
    """Represent an Azure subscription"""

    identifier: str
    name: str

    def fetch_assignments(self) -> list[Assignment]:
        """Fetch role assignments of the subscription"""
        with AuthorizationManagementClient(
            DefaultAzureCredential(), self.identifier
        ) as client:
            response = client.role_assignments.list_for_subscription()
            assignments: list[Assignment] = list()
            for i in response:
                if i.principal_type not in [t for t in PrincipalType]:
                    continue
                assignments.append(
                    Assignment(
                        identifier=i.id,
                        subscription_identifier=self.identifier,
                        principal_type=PrincipalType(i.principal_type),
                        principal_identifier=i.principal_id,
                        role_definition_identifier=i.role_definition_id,
                    )
                )
        return assignments

    def merge_record(self, session: Session) -> None:
        """Record the subscription to database"""
        query = "MERGE (:SUBSCRIPTION {id: '%s', name: '%s'})" % (
            self.identifier,
            self.name,
        )
        with session.begin_transaction() as tx:
            tx.run(query)
            tx.commit()
