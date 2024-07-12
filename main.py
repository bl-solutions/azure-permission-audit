import asyncio
import logging
import os
import sys
from enum import StrEnum
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.subscription import SubscriptionClient
from dotenv import load_dotenv
from msgraph import GraphServiceClient
from neo4j import GraphDatabase, ManagedTransaction, Session
from pydantic import BaseModel


class Node(StrEnum):
    SUBSCRIPTION = "SUBSCRIPTION"
    GROUP = "GROUP"
    USER = "USER"


class Relation(StrEnum):
    # MEMBER_OF = "MEMBER_OF"
    ASSIGNMENT = "ASSIGNMENT"


class SubscriptionNode(BaseModel):
    id: str
    name: str = ""

    @property
    def clean_id(self) -> str:
        return self.id.split("/")[-1]


class GroupNode(BaseModel):
    id: str
    name: str = ""

    async def fetch_name(self) -> None:
        scopes = ["https://graph.microsoft.com/.default"]
        client = GraphServiceClient(DefaultAzureCredential(), scopes)
        result = await client.groups.by_group_id(self.id).get()
        self.name = result.display_name


class UserNode(BaseModel):
    id: str
    name: str = ""

    async def fetch_name(self) -> None:
        scopes = ["https://graph.microsoft.com/.default"]
        client = GraphServiceClient(DefaultAzureCredential(), scopes)
        result = await client.users.by_user_id(self.id).get()
        self.name = result.display_name


class Assignment(BaseModel):
    id: str
    subscription_id: str
    role_id: str
    principal_id: str
    principal_type: str
    role_name: str = ""

    def fetch_role_name(self) -> None:
        with AuthorizationManagementClient(
            DefaultAzureCredential(), self.subscription_id
        ) as client:
            response = client.role_definitions.get_by_id(self.role_id)
            self.role_name = response.role_name

    @property
    def principal_node_type(self) -> Optional[Node]:
        match self.principal_type:
            case "Group":
                return Node.GROUP
            case "User":
                return Node.USER
            case _:
                return None


async def main():
    global logger
    logger = init_logger()
    load_dotenv()

    neo4j_driver_properties = {
        "uri": os.environ["NEO4J_URI"],
        "auth": (os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
    }

    neo4j_session_properties = {
        "database": os.environ["NEO4J_DATABASE"],
    }

    with GraphDatabase.driver(**neo4j_driver_properties) as driver:
        try:
            driver.verify_connectivity()
            logger.info("Database connection established")
        except Exception as e:
            logger.exception(e)
            exit(1)

        with driver.session(**neo4j_session_properties) as session:
            logger.info("Applying constraints...")
            apply_constraints(session)

            logger.info("Listing Azure subscriptions...")
            subscriptions = build_subscription_nodes()
            logger.info("Recording Azure subscriptions...")
            session.execute_write(record_subscriptions, subscriptions)

            logger.info("Listing role assignments...")
            assignments: list[Assignment] = list()
            for subscription in subscriptions:
                # To comment
                # if len(assignments) > 0:
                #     break
                # End
                assignments.extend(build_assignment_relations(subscription))

            logger.info("Listing groups...")
            group_ids = unique(
                [a.principal_id for a in assignments if a.principal_type == "Group"]
            )
            groups = await build_group_nodes(group_ids)
            logger.info("Recording groups...")
            session.execute_write(record_groups, groups)

            logger.info("Listing users...")
            user_ids = unique(
                [a.principal_id for a in assignments if a.principal_type == "User"]
            )
            users = await build_user_nodes(user_ids)
            logger.info("Recording users...")
            session.execute_write(record_users, users)

            logger.info("Recording role assignments...")
            session.execute_write(record_assignments, assignments)

            logger.info("Getting role names...")
            session.execute_write(update_role_name_records, assignments)


def init_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    return logger


def apply_constraints(session: Session) -> None:
    try:
        session.execute_write(add_subscription_constraint)
    except Exception as e:
        logger.debug("Subscription constraint already applied")
    try:
        session.execute_write(add_group_constraint)
    except Exception as e:
        logger.debug("Group constraint already applied")
    try:
        session.execute_write(add_user_constraint)
    except Exception as e:
        logger.debug("User constraint already applied")
    # try:
    #     session.execute_write(add_assignment_constraint)
    # except Exception as e:
    #     logger.debug("Assignment constraint already applied")


def unique(items: list[str]) -> list[str]:
    items_set = set(items)
    return list(items_set)


def build_subscription_nodes() -> list[SubscriptionNode]:
    with SubscriptionClient(DefaultAzureCredential()) as client:
        response = client.subscriptions.list()
        subscriptions = list()
        for subscription in response:
            subscription_id = subscription.id
            subscription_name = subscription.display_name
            subscriptions.append(
                SubscriptionNode(id=subscription_id, name=subscription_name)
            )
    return subscriptions


async def build_group_nodes(group_ids: list[str]) -> list[GroupNode]:
    group_nodes = list()
    for group_id in group_ids:
        group_node = GroupNode(id=group_id)
        await group_node.fetch_name()
        group_nodes.append(group_node)
    return group_nodes


async def build_user_nodes(user_ids: list[str]) -> list[UserNode]:
    user_nodes = list()
    for user_id in user_ids:
        user_node = UserNode(id=user_id)
        await user_node.fetch_name()
        user_nodes.append(user_node)
    return user_nodes


def build_assignment_relations(
    subscription_node: SubscriptionNode,
) -> list[Assignment]:
    with AuthorizationManagementClient(
        DefaultAzureCredential(), subscription_node.clean_id
    ) as client:
        logger.info("Listing assignments on %s" % subscription_node.id)
        assignments = client.role_assignments.list_for_subscription()
        assignment_relations = list()
        for assignment in assignments:
            if (
                assignment.principal_type != "Group"
                and assignment.principal_type != "User"
            ):
                continue
            properties = {
                "id": assignment.id,
                "subscription_id": subscription_node.id,
                "role_id": assignment.role_definition_id,
                "principal_id": assignment.principal_id,
                "principal_type": assignment.principal_type,
            }
            assignment_relation = Assignment(**properties)
            assignment_relations.append(assignment_relation)
    return assignment_relations


def record_subscriptions(
    tx: ManagedTransaction, subscriptions: list[SubscriptionNode]
) -> None:
    base_query = "MERGE (:%s {id: '%s', name: '%s'})"
    query = ""
    for subscription in subscriptions:
        query = (
            query
            + "\n"
            + base_query % (Node.SUBSCRIPTION, subscription.id, subscription.name)
        )
    logger.debug(query)
    tx.run(query)


def record_groups(tx: ManagedTransaction, groups: list[GroupNode]) -> None:
    base_query = "MERGE (:%s {id: '%s', name: '%s'})"
    query = ""
    for group in groups:
        query = query + "\n" + base_query % (Node.GROUP, group.id, group.name)
    logger.debug(query)
    tx.run(query)


def record_users(tx: ManagedTransaction, users: list[UserNode]) -> None:
    base_query = "MERGE (:%s {id: '%s', name: '%s'})"
    query = ""
    for user in users:
        query = query + "\n" + base_query % (Node.USER, user.id, user.name)
    logger.debug(query)
    tx.run(query)


def update_role_name_records(
    tx: ManagedTransaction, assignments: list[Assignment]
) -> None:
    base_query = """
        MATCH (:%s {id: '%s'})-[r:%s {id: '%s', role_id: '%s'}]-(:%s {id: '%s'})
        SET r.role_name = '%s'
    """
    for assignment in assignments:
        principal_node_type = assignment.principal_node_type
        if not principal_node_type:
            continue
        assignment.fetch_role_name()

        query = base_query % (
            Node.SUBSCRIPTION,
            assignment.subscription_id,
            Relation.ASSIGNMENT,
            assignment.id,
            assignment.role_id,
            principal_node_type,
            assignment.principal_id,
            assignment.role_name,
        )
        logger.debug(query)
        tx.run(query)


def record_assignments(tx: ManagedTransaction, assignments: list[Assignment]) -> None:
    base_query = """
        MATCH (s:%s {id: '%s'})
        MATCH (n:%s {id: '%s'})
        MERGE (s)-[r:%s {id: '%s', role_id: '%s'}]->(n)
    """
    query = ""
    for assignment in assignments:
        principal_node_type = assignment.principal_node_type
        if not principal_node_type:
            continue

        query = base_query % (
            Node.SUBSCRIPTION,
            assignment.subscription_id,
            principal_node_type,
            assignment.principal_id,
            Relation.ASSIGNMENT,
            assignment.id,
            assignment.role_id,
        )

        logger.debug(query)
        tx.run(query)


def add_subscription_constraint(tx: ManagedTransaction) -> None:
    constraint = (
        "CREATE CONSTRAINT subscription_id_unique FOR (n:%s) REQUIRE n.id IS UNIQUE"
        % (Node.SUBSCRIPTION)
    )
    tx.run(constraint)


def add_group_constraint(tx: ManagedTransaction) -> None:
    constraint = (
        "CREATE CONSTRAINT group_id_unique FOR (n:%s) REQUIRE n.id IS UNIQUE"
        % (Node.GROUP)
    )
    tx.run(constraint)


def add_user_constraint(tx: ManagedTransaction) -> None:
    constraint = (
        "CREATE CONSTRAINT user_id_unique FOR (n:%s) REQUIRE n.id IS UNIQUE"
        % (Node.USER)
    )
    tx.run(constraint)


def add_assignment_constraint(tx: ManagedTransaction) -> None:
    constraint = (
        "CREATE CONSTRAINT assignment_id_unique FOR [r:%s] REQUIRE r.id IS UNIQUE"
        % (Node.USER)
    )
    tx.run(constraint)


if __name__ == "__main__":
    asyncio.run(main())
