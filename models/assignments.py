from azure.identity import DefaultAzureCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from neo4j import Session
from pydantic import BaseModel

from .principals import PrincipalType


class Assignment(BaseModel):
    """Represent a role assignment"""

    identifier: str
    subscription_identifier: str
    principal_identifier: str
    principal_type: PrincipalType
    role_definition_identifier: str
    role_name: str = ""

    def fetch_role_name(self) -> None:
        """Fetch the role name"""
        with AuthorizationManagementClient(
            DefaultAzureCredential(), self.subscription_identifier
        ) as client:
            response = client.role_definitions.get_by_id(
                self.role_definition_identifier
            )
            self.role_name = response.role_name

    def merge_record(self, session: Session) -> None:
        """Record the assignment to database"""
        query = """
            MATCH (s:SUBSCRIPTION {id: '%s'})
            MATCH (n:%s {id: '%s'})
            MERGE (s)-[:ASSIGNMENT {id: '%s', role_id: '%s'}]->(n)
        """ % (
            self.subscription_identifier,
            self.principal_type.upper(),
            self.principal_identifier,
            self.identifier,
            self.role_definition_identifier,
        )
        with session.begin_transaction() as tx:
            tx.run(query)
            tx.commit()

    def update_record_role_name(self, session: Session) -> None:
        """Update the role name on assignment record"""
        query = """
            MATCH (:SUBSCRIPTION {id: '%s'})-[r:ASSIGNMENT {id: '%s', role_id: '%s'}]-(:%s {id: '%s'})
                    SET r.role_name = '%s'
        """ % (
            self.subscription_identifier,
            self.identifier,
            self.role_definition_identifier,
            self.principal_type.upper(),
            self.principal_identifier,
            self.role_name,
        )
        with session.begin_transaction() as tx:
            tx.run(query)
            tx.commit()
