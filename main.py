import asyncio
import concurrent.futures as cf
import logging
import os
import sys
import time

from azure.identity import DefaultAzureCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.subscription import SubscriptionClient
from dotenv import load_dotenv
from neo4j import GraphDatabase, ManagedTransaction, Session

MAX_WORKERS = 4

from models import (Assignment, GroupPrincipal, PrincipalType, Subscription,
                    UserPrincipal)


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
            subscriptions = fetch_subscriptions()

            logger.info("Recording Azure subscriptions...")
            [s.merge_record(session) for s in subscriptions]

            logger.info("Listing role assignments...")
            assignments = fetch_all_subscription_role_assignments(
                [s.identifier for s in subscriptions]
            )

            logger.info("Listing groups...")
            group_ids = unique(
                [
                    a.principal_identifier
                    for a in assignments
                    if a.principal_type is PrincipalType.GROUP
                ]
            )
            groups = [GroupPrincipal(identifier=i) for i in group_ids]
            logger.info("Recording groups...")
            [g.merge_record(session) for g in groups]

            logger.info("Listing users...")
            user_ids = unique(
                [
                    a.principal_identifier
                    for a in assignments
                    if a.principal_type is PrincipalType.USER
                ]
            )
            users = [UserPrincipal(identifier=i) for i in user_ids]
            logger.info("Recording users...")
            [u.merge_record(session) for u in users]

            logger.info("Recording role assignments...")
            [a.merge_record(session) for a in assignments]

            logger.info("Getting group members...")
            await asyncio.gather(
                *[record_group_members(session, group) for group in groups]
            )

            # Take a lot of time to fetch data based on
            # - internet connection
            # - msgraph response time
            # - the number of requests
            logger.info("Getting user names...")
            await asyncio.gather(*[user.fetch_name() for user in users])

            logger.info("Updating user names...")
            [u.update_record_name(session) for u in users]

            logger.info("Getting group names...")
            [await g.fetch_name() for g in groups]
            logger.info("Updating group names...")
            [g.update_record_name(session) for g in groups]

            logger.info("Getting role names...")
            [a.fetch_role_name() for a in assignments]
            logger.info("Updating role names...")
            [a.update_record_role_name(session) for a in assignments]


async def record_group_members(session: Session, group: GroupPrincipal) -> None:
    logger.info("Get members of %s group" % group.identifier)
    members = await group.fetch_members()
    for member in members:
        logger.debug("Recording member: %s" % member)
        member.merge_record(session)
        group.merge_member_record(session, member)
        if isinstance(member, GroupPrincipal):
            await record_group_members(session, member)


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


def unique(items: list[str]) -> list[str]:
    items_set = set(items)
    return list(items_set)


def fetch_subscriptions() -> list[Subscription]:
    subscriptions: list[Subscription] = list()
    with SubscriptionClient(DefaultAzureCredential()) as client:
        for s in client.subscriptions.list():
            subscriptions.append(
                Subscription(identifier=s.subscription_id, name=s.display_name)
            )
    return subscriptions


def add_subscription_constraint(tx: ManagedTransaction) -> None:
    constraint = "CREATE CONSTRAINT subscription_id_unique FOR (n:SUBSCRIPTION) REQUIRE n.id IS UNIQUE"
    tx.run(constraint)


def add_group_constraint(tx: ManagedTransaction) -> None:
    constraint = (
        "CREATE CONSTRAINT group_id_unique FOR (n:GROUP) REQUIRE n.id IS UNIQUE"
    )
    tx.run(constraint)


def add_user_constraint(tx: ManagedTransaction) -> None:
    constraint = "CREATE CONSTRAINT user_id_unique FOR (n:USER) REQUIRE n.id IS UNIQUE"
    tx.run(constraint)


def fetch_all_subscription_role_assignments(
    subscription_ids: list[str],
) -> list[Assignment]:
    assignments: list[Assignment] = list()
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(fetch_subscription_role_assignments, subscription_ids)
        [assignments.extend(r) for r in results]
    return assignments


def fetch_subscription_role_assignments(subscription_id: str) -> list[Assignment]:
    logger.info("Get role assignments for subscription %s" % subscription_id)
    with AuthorizationManagementClient(
        DefaultAzureCredential(), subscription_id
    ) as client:
        response = client.role_assignments.list_for_subscription()
        return [
            Assignment(
                identifier=assignment.id,
                subscription_identifier=subscription_id,
                principal_type=PrincipalType(assignment.principal_type),
                principal_identifier=assignment.principal_id,
                role_definition_identifier=assignment.role_definition_id,
            )
            for assignment in response
            if assignment.principal_type.lower() in [t.lower() for t in PrincipalType]
        ]


if __name__ == "__main__":
    asyncio.run(main())
