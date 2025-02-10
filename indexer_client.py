
import json
import logging
from time import sleep
import requests
import urllib3
from requests.adapters import HTTPAdapter


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def index_post(url: str, credentials: dict, data: dict | str, session: requests.Session = None) -> dict:
    """Send a POST request to index documents."""
    logger = logging.getLogger(__name__)
    headers = {'Content-Type': 'application/json'}
    if not session:
        session = get_retry_session()

    if isinstance(data, dict):
        data = json.dumps(data)

    try:
        response = session.post(url,
                                data=data,
                                headers=headers,
                                verify=False,
                                auth=(credentials.get('user'), credentials.get('password')))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException:
        logger.exception(f"Failed to index data to {url}.")
        return {}


def index_post_batch(url: str, credentials: dict, data: list, index: str, batch_size=1000, session=None) -> list[dict]:
    """Send a POST request to index documents in larger chunks."""
    data_length = len(data)

    def send_request(_data: list) -> dict:
        payload = __set_bulk_payload(_data, index)
        return index_post(url, credentials, payload, session)

    if data_length <= batch_size:
        if not isinstance(data, list):
            data = [data]
        return [send_request(data)]

    # Split data into larger batches
    responses = []
    for i in range(0, data_length, batch_size):
        chunk = data[i:i + batch_size]
        response = send_request(chunk)
        responses.append(response)
        sleep(0.01)

    return responses


def get_retry_session() -> requests.Session:
    session = requests.Session()
    status = [500, 502, 503, 504]
    retries = urllib3.Retry(total=5, backoff_factor=1, status_forcelist=status)
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

# Private functions


def __set_bulk_payload(documents: list[dict], index: str) -> str:
    payload = ""
    for doc in documents:
        metadata = {"index": {"_index": index}}
        payload += json.dumps(metadata) + "\n"
        payload += json.dumps(doc) + "\n"
    return payload
