
import logging
from typing import Generator
from indexer_client import get_retry_session, index_post, index_post_batch

BATCH_SIZE = 10000


def index_data(cluster_url: str, creds: dict, index: str, data: dict) -> list:
    url = f"{cluster_url}/{index}/_bulk"
    return index_post_batch(url, creds, data, index)


def index_data_from_generator(
        cluster_url: str,
        creds: dict,
        index: str,
        generator: Generator,
        amount: int,
        data: list = [],
        _return: bool = False,
        bulk: bool = True) -> None:
    """Generate and index packages in larger batches synchronously."""
    logger = logging.getLogger(__name__)
    doc_url = f"{cluster_url}/{index}"
    session = get_retry_session()
    batch = []  # Collect documents before sending
    to_save = []
    count = 0  # Counter to track number of batches processed

    logger.info(f"Generating and indexing {amount} packages in batches of {BATCH_SIZE}...")
    for document in generator(amount, data):
        if not bulk:
            count += 1
            logger.info(f"Indexing document {count}.")
            index_post(f"{doc_url}/_doc", creds, document, session)
            continue
        batch.append(document)

        # Send when reaching batch size
        if len(batch) >= BATCH_SIZE:
            index_post_batch(f"{doc_url}/_bulk", creds, batch, index, BATCH_SIZE, session)
            count += 1
            # Log only every 10 batches
            if count % 10 == 0:
                logger.info(f"Indexed batch {count}.")
            if _return:
                to_save.extend(batch)
            batch.clear()  # Clear list for next batch

    # Send remaining documents if any
    if batch:
        index_post_batch(doc_url, creds, batch, index, BATCH_SIZE, session)
        count += 1
        # If you want to log the final batch only if it is a multiple of 10, do:
        if count % 10 == 0:
            logger.info(f"Indexed final batch {count}.")
        if _return:
            to_save.extend(batch)

    logger.info("All packages generated and indexed successfully.")
    return to_save


def update_group(cluster_url: str, creds: dict, index: str, group: str, new_group: str, max_docs: int = 150000000) -> dict:
    """Send a POST request to update the group of agents.

    This version adds query parameters to:
      - Ignore version conflicts (conflicts=proceed)
      - Optionally set a higher limit for the number of documents to update (max_docs)
    """
    # Append query parameters to the URL
    # Adjust max_docs as needed (here set to 15M, but change according to your scenario)
    url = f"{cluster_url}/{index}/_update_by_query?conflicts=proceed&max_docs={max_docs}&slices=auto&wait_for_active_shards=all"

    payload = {
        "profile": True,
        "timeout": "15m",
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
