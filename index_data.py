
import logging
from typing import Generator
from indexer_client import get_retry_session, index_post, index_post_batch

BATCH_SIZE = 10000


def index_data(cluster_url: str, creds: dict, index: str, data: dict) -> list:
    url = f"{cluster_url}/{index}/_bulk"
    return index_post_batch(url, creds, data, index)


def index_data_from_generator(cluster_url: str, creds: dict, index: str, generator: Generator, amount: int, data: list = [], _return=False) -> None:
    """Generate and index packages in larger batches synchronously."""
    logger = logging.getLogger(__name__)
    doc_url = f"{cluster_url}/{index}/_bulk"
    session = get_retry_session()
    batch = []  # Collect documents before sending
    to_save = []
    indexed = 0

    for document in generator(amount, data):
        batch.append(document)
        indexed += 1

        # Send when reaching batch size
        if len(batch) >= BATCH_SIZE:
            index_post_batch(doc_url, creds, batch, index, BATCH_SIZE, session)
            logger.info(f"Indexed {indexed} packages successfully.")
            if _return:
                to_save.extend(batch)
            batch.clear()  # Clear list for next batch

    # Send remaining documents if any
    if batch:
        index_post_batch(doc_url, creds, batch, index, BATCH_SIZE, session)
        logger.info(f"Indexed final {indexed} packages successfully.")
        if _return:
            to_save.extend(batch)

    logger.info("All packages generated and indexed successfully.")
    return to_save


def update_group(cluster_url: str, creds: dict, index: str, group: str, new_group: str) -> dict:
    """Send a POST request to update the group of agents."""
    url = f"{cluster_url}/{index}/_update_by_query"
    payload = {
        "profile": "true",
        "query": {
            "match": {
                "agent.groups": group
            }
        },
        "script": {
            "source": "ctx._source.agent.groups = params.newValue",
            "lang": "painless",
            "params": {
                "newValue": new_group
            }
        }
    }
    return index_post(url, creds, payload)


def force_merge(cluster_url: str, creds: dict) -> dict:
    url = f'{cluster_url}/_forcemerge'
    return index_post(url, creds, {})


def refresh_index(cluster_url: str, creds: dict, index: str) -> dict:
    url = f'{cluster_url}/{index}/_refresh'
    return index_post(url, creds, {})
