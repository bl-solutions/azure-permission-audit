from enum import StrEnum

from azure.identity import DefaultAzureCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.group import Group
from msgraph.generated.models.user import User
from neo4j import Session
from pydantic import BaseModel


class PrincipalType(StrEnum):
    USER = "User"
    GROUP = "Group"


class PrincipalInterface(BaseModel):
    """Interface for principal types"""

    identifier: str
    name: str = ""

    @property
    def principal_type(self) -> PrincipalType:
        """Returns the type of the principal"""
        pass

    def fetch_name(self) -> None:
        """Fetch the principal name"""
        pass

    def merge_record(self, session: Session) -> None:
        """Record principal to the database"""
        search = "MATCH (n:%s {id: '%s'}) RETURN n" % (self.principal_type.upper(), self.identifier)
        query = "MERGE (:%s {id: '%s', name: '%s'})" % (
            self.principal_type.upper(),
            self.identifier,
            self.name
        )
        with session.begin_transaction() as tx:
            result = tx.run(search)
            if result.single():
                return
            tx.run(query)
            tx.commit()

    def update_record_name(self, session: Session) -> None:
        """Update the principal name"""
        query = "MATCH (n:%s {id: '%s'}) SET n.name = '%s'" % (
            self.principal_type.upper(),
            self.identifier,
            self.name,
        )
        with session.begin_transaction() as tx:
            tx.run(query)
            tx.commit()


class UserPrincipal(PrincipalInterface):
    """Represent a User principal"""

    @property
    def principal_type(self) -> PrincipalType:
        return PrincipalType.USER

    async def fetch_name(self) -> None:
        scopes = ["https://graph.microsoft.com/.default"]
        client = GraphServiceClient(DefaultAzureCredential(), scopes)
        result = await client.users.by_user_id(self.identifier).get()
        self.name = result.display_name


class GroupPrincipal(PrincipalInterface):
    """Represent a Group principal"""

    @property
    def principal_type(self) -> PrincipalType:
        return PrincipalType.GROUP

    async def fetch_name(self):
        scopes = ["https://graph.microsoft.com/.default"]
        client = GraphServiceClient(DefaultAzureCredential(), scopes)
        result = await client.groups.by_group_id(self.identifier).get()
        self.name = result.display_name

    async def fetch_members(self) -> list[PrincipalInterface]:
        scopes = ["https://graph.microsoft.com/.default"]
        client = GraphServiceClient(DefaultAzureCredential(), scopes)
        result = await client.groups.by_group_id(self.identifier).members.get()
        principals: list[PrincipalInterface] = list()
        for v in result.value:
            if isinstance(v, Group):
                principals.append(GroupPrincipal(identifier=v.id, name=v.display_name))
            elif isinstance(v, User):
                principals.append(UserPrincipal(identifier=v.id, name=v.display_name))
            else:
                continue
        return principals

    def merge_member_record(self, session: Session, member: PrincipalInterface) -> None:
        query = query = """
            MATCH (g:%s {id: '%s'})
            MATCH (n:%s {id: '%s'})
            MERGE (n)-[:MEMBER_OF]->(g)
        """ % (self.principal_type.upper(), self.identifier, member.principal_type.upper(), member.identifier)
        with session.begin_transaction() as tx:
            tx.run(query)
            tx.commit()
