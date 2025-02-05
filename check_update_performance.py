#!/bin/python3

import datetime
import json
import logging
from requests.adapters import HTTPAdapter
import random
from time import sleep
import requests
import urllib3

# Constants and Configuration
LOG_FILE = 'update_performance.log'
GENERATED_AGENTS = 'generated_agents.json'
GENERATED_PACKAGES = 'generated_packages.json'

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
        response = session.post(url, data=data, headers=headers, verify=False, auth=(user, password))
        response.raise_for_status()
        logger.info("Data indexed successfully: %s", response.json())
        return response.json()
    except requests.exceptions.RequestException:
        logger.exception("Error indexing data.")
        return {}


def index_post_batch(url: str, user: str, password: str, data: list, index, batch_size=5000):
    """Send a POST request to index documents in chunks to avoid '413 Request Entity Too Large' error."""
    session = get_retry_session()

    # Split data into smaller batches
    for i in range(0, len(data), batch_size):
        chunk = data[i:i + batch_size]
        # Convert batch to bulk format
        payload = set_bulk_payload(chunk, index)
        response = index_post(url, user, password, payload, session)
        logger.info(f"Indexed batch {i // batch_size + 1} of {len(data) // batch_size + 1} successfully.")
        logger.debug("Response: %s", response)

    return {"status": "success"}


def save_generated_data(data, filename):
    """Save generated data to a file."""
    try:
        with open(filename, 'w') as outfile:
            json.dump(data, outfile, indent=2)
        logger.info("Generated data saved successfully.")
    except IOError:
        logger.exception("Error saving data to file.")


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


def generate_agents(number: int) -> list:
    """Generate a list of random agent events."""
    logger.info("Generating %d random agents...", number)
    data = []
    num_windows = int(0.5 * number)
    num_macos = int(0.15 * number)
    num_linux = number - num_windows - num_macos

    for _ in range(num_windows):
        data.append({'agent': generate_random_agent('windows')})
    for _ in range(num_macos):
        data.append({'agent': generate_random_agent('macos')})
    for _ in range(num_linux):
        data.append({'agent': generate_random_agent('linux')})

    logger.info("Random data generation complete.")
    return data


def get_user_input(prompt, default):
    """Get user input with a default fallback."""
    return input(f"{prompt} (default: '{default}'): ") or default


def set_bulk_payload(documents, index):
    payload = ""
    for doc in documents:
        metadata = {"index": {"_index": index,
                              "_id": doc.get('agent', {}).get('id')}}
        payload += json.dumps(metadata) + "\n"
        payload += json.dumps(doc) + "\n"
    return payload


def generate_package_doc(agents, amount):
    """Send restart documents to a list of agent IDs."""
    packages = [
        {
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
                "description": "tmux is a \"terminal multiplexer.\"  It enables a number of terminals (or \
                                windows) to be accessed and controlled from a single terminal.  tmux is \
                                intended to be a simple, modern, BSD-licensed alternative to programs such \
                                as GNU Screen.",
                "installed": "1738151465",
                "name": "tmux",
                "path": " ",
                "size": 1166902,
                "type": "rpm",
                "version": "3.2a-5.el9"
            }
        }
        for agent in agents
        for _ in range(amount)
    ]

    if not packages:
        logger.warning("No packages generated to send.")
        return
    return packages


def index_packages(cluster_url, user, password, packages):
    url = f"{cluster_url}/{INDEX_PACKAGES}/_bulk"
    response = index_post_batch(url, user, password, packages, INDEX_PACKAGES, batch_size=10000)
    logger.debug("Response: %s", response)
    logger.info(f"Successfully sent {len(packages)} packages.")
    return response


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


def index_agents(cluster_url, user, password, data):
    url = f"{cluster_url}/{INDEX_AGENTS}/_bulk"
    index_post_batch(url, user, password, data, INDEX_AGENTS)
    return data


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


def generate_package_doc_chunk(agent_chunk, amount):
    """Generate package documents for a chunk of agents."""
    return [
        {
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
                "description": "tmux is a terminal multiplexer.",
                "installed": "1738151465",
                "name": "tmux",
                "path": " ",
                "size": 1166902,
                "type": "rpm",
                "version": "3.2a-5.el9"
            }
        }
        for agent in agent_chunk
        for _ in range(amount)
    ]


def generate_packages_in_batches(agents, amount, batch_size):
    """Generate package documents in smaller batches to avoid memory overflow."""
    total_packages = len(agents) * amount
    logger.info(f"Generating {total_packages} package documents in batches of {
                batch_size}...")

    for i in range(0, len(agents), batch_size):
        batch_agents = agents[i:i + batch_size]
        packages = generate_package_doc(batch_agents, amount)
        logger.info(f"Generated batch {i // batch_size + 1} of {len(agents) // batch_size + 1} packages.")
        yield packages


def generate_and_index_packages(cluster_url, user, password, agents, num_packages, batch_size=50000):
    """Generate and index packages in batches synchronously."""
    doc_url = f"{cluster_url}/{INDEX_PACKAGES}/_bulk"

    for batch in generate_packages_in_batches(agents, num_packages, batch_size):
        if not batch:
            continue
        response = index_post_batch(doc_url, user, password, batch, INDEX_PACKAGES, batch_size=10000)
        logger.info(f"Indexed {len(batch)} packages successfully.")
        logger.debug("Response: %s", response)
        save_generated_data(batch, GENERATED_PACKAGES)


def main():
    # User inputs
    ip = input(f"Enter the IP of your Indexer (default: '{DEFAULT_IP}'): ") or DEFAULT_IP
    port = input(f"Enter the port of your Indexer (default: '{DEFAULT_PORT}'): ") or DEFAULT_PORT
    user = input(f"Enter user (default: '{DEFAULT_USER}'): ") or DEFAULT_USER
    password = input(f"Enter password (default: '{DEFAULT_PASSWORD}'): ") or DEFAULT_PASSWORD

    cluster_url = f"https://{ip}:{port}"

    try:
        num_agents = int(input("Enter the number of agents to generate (0 to load from file): "))
        num_packages = int(input("Enter the number of packages to generate for each agent (0 to load from file): "))
        update_groups = input("Update groups? (y/n): ").lower() == 'y'
    except ValueError:
        logger.error("Invalid input. Please enter valid numbers.")
        return

    # Load or Generate Agents
    if num_agents == 0:
        logger.info("Loading existing agents from file...")
        try:
            with open(GENERATED_AGENTS, 'r') as infile:
                agents = json.load(infile)
            logger.info("Agents loaded successfully.")
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Error loading agents file: {e}")
            return
    else:
        logger.info("Generating new agents...")
        agents = generate_agents(num_agents)
        save_generated_data(agents, GENERATED_AGENTS)
        index_agents(cluster_url, user, password, agents)
        refresh_index(cluster_url, INDEX_AGENTS, user, password)
        sleep(2)

    # Load or Generate Packages
    if num_packages == 0:
        logger.info("Loading existing package data from file...")
        try:
            with open(GENERATED_PACKAGES, 'r') as infile:
                packages = json.load(infile)
            logger.info(f"Loaded {len(packages)} packages successfully.")
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Error loading packages file: {e}")
            return

        # Index the loaded packages
        logger.info("Indexing loaded packages into Elasticsearch...")
        index_packages(cluster_url, user, password, packages)
    else:
        logger.info("Generating and indexing packages...")
        generate_and_index_packages(cluster_url, user, password, agents, num_packages)

    force_merge(cluster_url, user, password)
    refresh_index(cluster_url, INDEX_PACKAGES, user, password)
    sleep(2)

    if update_groups:
        update_groups_get_performance(cluster_url, user, password)


if __name__ == "__main__":
    main()
