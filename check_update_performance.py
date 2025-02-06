import datetime
import json
import logging
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from requests.adapters import HTTPAdapter
import random
from time import sleep
import requests
import urllib3

# Constants and Configuration
LOG_FILE = 'update_performance.log'
GENERATED_AGENTS = 'generated_agents.json'

# Default values
INDEX_AGENTS = "wazuh-agents"
INDEX_PACKAGES = "wazuh-states-inventory-packages"
DEFAULT_USER = "admin"
DEFAULT_PASSWORD = "admin"
DEFAULT_IP = "127.0.0.1"
DEFAULT_PORT = "9200"

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Suppress warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Generate 500 unique group names
unique_groups = [f'group{i}' for i in range(500)]
random.shuffle(unique_groups)


def get_retry_session():
    session = requests.Session()
    retries = urllib3.Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def index_post(url: str, user: str, password: str, data: dict | str, session: requests.Session = None) -> dict:
    """Send a POST request to index documents."""
    headers = {'Content-Type': 'application/json'}
    if not session:
        session = get_retry_session()

    if isinstance(data, dict):
        data = json.dumps(data)

    try:
        response = session.post(
            url, data=data, headers=headers, verify=False, auth=(user, password))
        response.raise_for_status()
        logger.debug("Data indexed: %s", response.json())
        return response.json()
    except requests.exceptions.RequestException:
        logger.exception("Error indexing data.")
        return {}


def index_post_batch(url: str, user: str, password: str, data: list, index: str, batch_size=1000, session=None):
    """Send a POST request to index documents in larger chunks."""
    data_length = len(data)

    def send_request(_data):
        payload = set_bulk_payload(_data, index)
        return index_post(url, user, password, payload, session)

    if data_length <= batch_size:
        if not isinstance(data, list):
            data = [data]
        return send_request(data)

    # Split data into larger batches
    responses = []
    for i in range(0, data_length, batch_size):
        chunk = data[i:i + batch_size]
        response = send_request(chunk)
        responses.append(response)
        logger.debug(f"Indexed batch {
            i // batch_size + 1} of {data_length // batch_size} successfully.")

    return responses


def set_bulk_payload(documents, index):
    payload = ""
    for doc in documents:
        metadata = {"index": {"_index": index,
                              "_id": doc.get('agent', {}).get('id')}}
        payload += json.dumps(metadata) + "\n"
        payload += json.dumps(doc) + "\n"
    return payload


# Indexing functions

def index_agents(cluster_url, user, password, data):
    url = f"{cluster_url}/{INDEX_AGENTS}/_bulk"
    index_post_batch(url, user, password, data, INDEX_AGENTS)
    return data


def generate_and_index_packages_parallel(cluster_url, user, password, agents, num_packages, batch_size=10000, num_processes=None):
    """Run generate_and_index_packages in multiple processes by splitting the agents list."""

    def worker(agent_subset):
        """Process worker function to handle a subset of agents."""
        generate_and_index_packages(cluster_url, user, password, agent_subset, num_packages, batch_size)

    # Set number of processes (default: CPU count)
    if num_processes is None:
        num_processes = multiprocessing.cpu_count()

    # Split agents into equal chunks for each process
    chunk_size = max(1, len(agents) // num_processes)
    agent_chunks = [agents[i:i + chunk_size] for i in range(0, len(agents), chunk_size)]

    # Use multiprocessing Pool
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = [pool.apply_async(worker, (chunk,)) for chunk in agent_chunks]

        # Ensure all processes complete
        for result in results:
            result.get()  # Wait for process to finish and catch any errors

    logger.info("All processes have completed package generation and indexing.")


def generate_and_index_packages(cluster_url, user, password, agents, num_packages, batch_size=10000):
    """Generate and index packages in larger batches synchronously."""
    doc_url = f"{cluster_url}/{INDEX_PACKAGES}/_bulk"
    session = get_retry_session()
    batch = []  # Collect documents before sending
    total_packages = len(agents) * num_packages
    indexed_packages = 0

    logger.info(f"Generating {num_packages} packages for each of the {len(agents)} agents...")

    for package_doc in generate_package_doc(agents, num_packages):
        batch.append(package_doc)
        indexed_packages += 1

        # Send when reaching batch size
        if len(batch) >= batch_size:
            index_post_batch(doc_url, user, password, batch, INDEX_PACKAGES, batch_size, session)
            logger.info(f"Indexed {indexed_packages} packages successfully. {indexed_packages / total_packages * 100:.2f}%")
            batch.clear()  # Clear list for next batch
            sleep(0.01)

    # Send remaining documents if any
    if batch:
        index_post_batch(doc_url, user, password, batch, INDEX_PACKAGES, batch_size, session)
        logger.info(f"Indexed final {indexed_packages} packages successfully. {indexed_packages / total_packages * 100:.2f}%")

    logger.info("All packages generated and indexed successfully.")


# Update groups

def update_group(cluster_url, user, password, group, new_group):
    """Send a POST request to update the group of agents."""
    url = f"{cluster_url}/{INDEX_PACKAGES}/_update_by_query"
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
    response = index_post(url, user, password, payload)
    logger.info("Group updated successfully.")
    logger.info("Response: %s", response)
    return response


def update_groups_get_performance(cluster_url, user, password):
    groups = ['windows', 'linux', 'macos']
    new_group = 'new-group'
    results = []
    print("Updating groups...\n")
    for group in groups:
        start = datetime.datetime.now()
        result = update_group(cluster_url, user,
                              password, group, f"{new_group}-{group}")
        end = datetime.datetime.now()
        print(f"Time taken to update group {group}: {end - start} seconds")
        results.append(result)
        sleep(5)
    [print(result) for result in results]


# Force merge and refresh index

def force_merge(cluster_url, user, password):
    url = f'{cluster_url}/_forcemerge'
    response = index_post(url, user, password, {})
    logger.debug("Response: %s", response)
    logger.info('Force merge completed successfully.')


def refresh_index(cluster_url, index, user, password):
    url = f'{cluster_url}/{index}/_refresh'
    response = index_post(url, user, password, {})
    logger.debug("Response: %s", response)
    logger.info('Index refreshed successfully.')


# Package generation

def generate_package_doc(agents, amount):
    """Send restart documents to a list of agent IDs."""
    for agent in agents:
        for _ in range(amount):
            yield {
                "agent": {
                    "id": agent.get('agent', {}).get('id'),
                    "name": agent.get('agent', {}).get('name'),
                    "groups": agent.get('agent', {}).get('groups'),
                    "type": agent.get('agent', {}).get('type'),
                    "version": agent.get('agent', {}).get('version'),
                    "host": {
                        "architecture": agent.get('agent', {}).get('host', {}).get('architecture'),
                        "hostname": agent.get('agent', {}).get('host', {}).get('hostname'),
                        "ip": agent.get('agent', {}).get('host', {}).get('ip'),
                        "os": {
                            "name": agent.get('agent', {}).get('host', {}).get('os', {}).get('name'),
                            "type": agent.get('agent', {}).get('host', {}).get('os', {}).get('type'),
                            "version": agent.get('agent', {}).get('host', {}).get('os', {}).get('version')
                        }
                    },
                },
                "@timestamp": datetime.datetime.now().isoformat(),
                "package": {
                    "architecture": agent.get('agent', {}).get('host', {}).get('architecture'),
                    "description": "tmux is a \"terminal multiplexer.\".",
                    "installed": "1738151465",
                    "name": "tmux",
                    "path": " ",
                    "size": 1166902,
                    "type": "rpm",
                    "version": "3.2a-5.el9"
                }
            }


# Agents generation

def generate_agents(number: int):
    """Generate a list of random agent events."""
    logger.info("Generating %d random agents...", number)
    num_windows = int(0.5 * number)
    num_macos = int(0.15 * number)
    num_linux = number - num_windows - num_macos

    logger.info(f"Generating agents: {num_windows} Windows, {
                num_macos} MacOS, {num_linux} Linux...")
    for _ in range(num_windows):
        yield {'agent': generate_random_agent('windows')}
    for _ in range(num_macos):
        yield {'agent': generate_random_agent('macos')}
    for _ in range(num_linux):
        yield {'agent': generate_random_agent('linux')}

    logger.info("Random data generation complete.")


def generate_random_agent(agent_type: str) -> dict:
    """Generate a random agent configuration."""
    agent_id = random.randint(0, 99999)
    return {
        'id': f'agent{agent_id}',
        'name': f'Agent{agent_id}',
        'type': agent_type,
        'version': f'v{random.randint(0, 9)}-stable',
        'status': random.choice(['active', 'inactive']),
        'last_login': generate_random_date(),
        'groups': generate_random_groups(agent_type),
        'key': f'key{agent_id}',
        'host': generate_random_host(agent_type)
    }


def generate_random_host(agent_type: str):
    """Generate a random host configuration."""
    os_families = {
        'linux': ['debian', 'ubuntu', 'centos', 'redhat'],
        'windows': ['windows'],
        'macos': ['macos', 'ios']
    }
    family = random.choice(os_families[agent_type])
    version = f'{random.randint(0, 99)}.{random.randint(0, 99)}'

    return {
        'architecture': random.choice(['x86_64', 'arm64']),
        'boot': {'id': f'boot{random.randint(0, 9999)}'},
        'cpu': {'usage': random.uniform(0, 100)},
        'disk': {
            'read': {'bytes': random.randint(0, 1_000_000)},
            'write': {'bytes': random.randint(0, 1_000_000)}
        },
        'domain': f'domain{random.randint(0, 999)}',
        'geo': {
            'city_name': random.choice(['San Francisco', 'New York', 'Berlin', 'Tokyo']),
            'continent_code': random.choice(['NA', 'EU', 'AS']),
            'continent_name': random.choice(['North America', 'Europe', 'Asia']),
            'country_iso_code': random.choice(['US', 'DE', 'JP']),
            'country_name': random.choice(['United States', 'Germany', 'Japan']),
            'location': {
                'lat': round(random.uniform(-90.0, 90.0), 6),
                'lon': round(random.uniform(-180.0, 180.0), 6)
            },
            'postal_code': f'{random.randint(10000, 99999)}',
            'region_name': f'Region {random.randint(0, 999)}',
            'timezone': random.choice(['PST', 'EST', 'CET', 'JST'])
        },
        'hostname': f'host{random.randint(0, 9999)}',
        'id': f'hostid{random.randint(0, 9999)}',
        'ip': ".".join(str(random.randint(1, 255)) for _ in range(4)),
        'mac': ":".join(f'{random.randint(0, 255):02x}' for _ in range(6)),
        'os': {
            'family': family,
            'full': f'{family} {version}',
            'kernel': f'kernel{random.randint(0, 999)}',
            'name': family,
            'platform': agent_type,
            'type': agent_type,
            'version': version
        },
        'uptime': random.randint(0, 1_000_000)
    }


def generate_random_date() -> str:
    """Generate a random date within the last 10 days."""
    start_date = datetime.datetime.now()
    end_date = start_date - datetime.timedelta(days=10)
    random_date = start_date + (end_date - start_date) * random.random()
    return random_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def generate_random_groups(extra_group=None) -> list[str]:
    """Return a list of randomly sampled groups."""
    groups = random.sample(unique_groups, 128)
    if extra_group:
        groups.pop()
        groups.append(extra_group)
    return groups


# Save generated data

def save_generated_data(data, filename):
    """Save generated data to a file."""
    try:
        with open(filename, 'w') as outfile:
            json.dump(data, outfile, indent=2)
        logger.info("Generated data saved successfully.")
    except IOError:
        logger.exception("Error saving data to file.")


# Main function

def main():
    # User inputs
    ip = input(f"Enter Indexer's IP (default: '{DEFAULT_IP}'): ") or DEFAULT_IP
    port = input(f"Enter Indexer's port (default: '{DEFAULT_PORT}'): ") or DEFAULT_PORT
    user = input(f"Enter user (default: '{DEFAULT_USER}'): ") or DEFAULT_USER
    password = input(f"Enter password (default: '{
                     DEFAULT_PASSWORD}'): ") or DEFAULT_PASSWORD

    try:
        num_agents = int(input("Amount of agents to generate (0 to load from file): "))
        num_packages = int(input("Amount of packages to generate for each agent: "))
        update_groups = input("Update groups? (y/n): ").lower() == 'y'
        num_threads = int(input("Enter the number of threads to use (default: 1): ") or 1)
    except ValueError:
        logger.error("Invalid input. Please enter valid numbers.")
        return

    cluster_url = f"https://{ip}:{port}"

    # Load or Generate Agents
    if num_agents == 0:
        print("Loading existing agents from file...")
        try:
            with open(GENERATED_AGENTS, 'r') as infile:
                agents = json.load(infile)
            print("Agents loaded successfully.")
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Error loading agents file: {e}")
            return
    else:
        print("Generating new agents...")
        agents = list(generate_agents(num_agents))
        save_generated_data(agents, GENERATED_AGENTS)

    if num_threads == 1:
        print("Generating and indexing packages synchronously...")
        generate_and_index_packages(cluster_url, user, password, agents, num_packages)
    else:
        print("Generating and indexing packages in parallel...")
        generate_and_index_packages_parallel(cluster_url, user, password, agents, num_packages, num_threads=num_threads)

    sleep(15)
    print("Forcing merge and refreshing index...")
    force_merge(cluster_url, user, password)
    sleep(5)
    refresh_index(cluster_url, INDEX_PACKAGES, user, password)
    sleep(5)

    if update_groups:
        update_groups_get_performance(cluster_url, user, password)


if __name__ == "__main__":
    main()
