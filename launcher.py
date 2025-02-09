import argparse
import datetime
import json
import logging
import logging.handlers
import multiprocessing
import queue
from concurrent.futures import ProcessPoolExecutor
from time import sleep

from data_generation import agents_generator, package_generator
from index_data import force_merge, index_data_from_generator, refresh_index, update_group

# Constants and Configuration
LOG_FILE = 'update_performance.log'
GENERATED_AGENTS = 'generated_agents.json'
INDEX_PACKAGES = "wazuh-states-inventory-packages"
INDEX_AGENTS = "wazuh-agents"

# Default values
DEFAULT_USER = "admin"
DEFAULT_PASSWORD = "admin"
DEFAULT_IP = "127.0.0.1"
DEFAULT_PORT = "9200"

# Set up a multiprocessing logging queue and a QueueListener in the main process
log_queue = multiprocessing.Queue()

# Configure a file handler for logging
file_handler = logging.FileHandler(LOG_FILE, mode="a")
file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file_handler.setFormatter(file_formatter)

# Set up the root logger (it will not have any handlers attached here)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Start the QueueListener (this runs in a background thread)
queue_listener = logging.handlers.QueueListener(log_queue, file_handler)
queue_listener.start()


def get_process_logger(name: str = None) -> logging.Logger:
    """
    Returns a logger that sends its output to the multiprocessing log queue.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # Clear any existing handlers to prevent duplicate logs.
    if not any(isinstance(h, logging.handlers.QueueHandler) for h in logger.handlers):
        queue_handler = logging.handlers.QueueHandler(log_queue)
        logger.addHandler(queue_handler)
    return logger


def worker(cluster_url: str, creds: dict, agents: list, num_packages: int) -> None:
    """Worker function for processing agent data."""
    logger = get_process_logger()
    logger.info(f"Generating {num_packages} packages for each of the {
                len(agents)} agents...")

    index_data_from_generator(
        cluster_url, creds, INDEX_PACKAGES, package_generator, num_packages, agents
    )

    logger.info("Finished generating and indexing packages.")


def generate_packages_parallel(cluster_url: str, creds: dict, agents: list, num_packages: int, processes=None) -> None:
    """Runs package generation using multiprocessing with a logging queue."""
    if processes is None:
        processes = multiprocessing.cpu_count()

    # Divide agents among processes.
    chunk_size = max(1, len(agents) // processes)
    agent_chunks = [agents[i:i + chunk_size]
                    for i in range(0, len(agents), chunk_size)]

    with ProcessPoolExecutor(max_workers=processes) as executor:
        futures = [
            executor.submit(worker, cluster_url, creds, chunk, num_packages)
            for chunk in agent_chunks
        ]
        for future in futures:
            future.result()  # Wait for all worker processes to complete

    # Log completion in the main process.
    root_logger.info(
        "All processes have completed package generation and indexing.")


def update_groups_get_performance(cluster_url: str, creds: dict) -> None:
    """Update groups and measure performance."""
    groups = ['windows', 'linux', 'macos']
    new_group = 'new-group'
    results = []
    print("Updating groups...\n")
    for group in groups:
        start = datetime.datetime.now()
        result = update_group(cluster_url, creds, INDEX_PACKAGES, group, f"{
                              new_group}-{group}")
        end = datetime.datetime.now()
        print(f"Time taken to update group {group}: {end - start} seconds")
        results.append(result)
        sleep(1)
    save_generated_data(results, 'update_results.json')


def save_generated_data(data, filename):
    """Save generated data to a file."""
    with open(filename, 'w') as outfile:
        json.dump(data, outfile, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Generate and index package data.")
    parser.add_argument("-ip", default=DEFAULT_IP, help="Indexer's IP address")
    parser.add_argument("-port", default=DEFAULT_PORT, help="Indexer's port")
    parser.add_argument("-user", default=DEFAULT_USER, help="Indexer's user")
    parser.add_argument("-password", default=DEFAULT_PASSWORD, help="Indexer's password")
    parser.add_argument("-agents", type=int, default=0, help="Number of agents to generate (0 to load from file)")
    parser.add_argument("-packages", type=int, default=100, help="Number of packages to generate for each agent")
    parser.add_argument("-threads", type=int, default=1, help="Number of threads (processes) to use")
    parser.add_argument("-update", type=bool, default=False, help="Update packages groups")
    args = parser.parse_args()

    credentials = {"user": args.user, "password": args.password}
    num_agents = args.agents
    num_packages = args.packages
    processes = args.threads
    update = args.update
    cluster_url = f"https://{args.ip}:{args.port}"

    # Load or Generate Agents
    if num_agents == 0:
        print("Loading existing agents from file...")
        try:
            with open(GENERATED_AGENTS, 'r') as infile:
                agents = json.load(infile)
            print("Agents loaded successfully.")
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error loading agents file: {e}")
            return
    else:
        print("Generating new agents...")
        # Assuming index_data_from_generator returns the generated data when _return=True.
        agents = index_data_from_generator(
            cluster_url, credentials, INDEX_AGENTS, agents_generator, num_agents, _return=True)
        save_generated_data(agents, GENERATED_AGENTS)

    print(f"Total packages to generate: {num_agents * num_packages}")
    if processes == 1:
        print("Generating and indexing packages synchronously...")
        index_data_from_generator(
            cluster_url, credentials, INDEX_PACKAGES, package_generator, num_packages, agents)
    else:
        print("Generating and indexing packages in parallel...")
        generate_packages_parallel(
            cluster_url, credentials, agents, num_packages, processes)

    # The following sleep calls might be necessary for your external index operations.
    if update:
        sleep(120)
        print("Forcing merge and refreshing index...")
        force_merge(cluster_url, credentials)
        sleep(120)
        refresh_index(cluster_url, credentials, INDEX_PACKAGES)
        sleep(120)
        update_groups_get_performance(cluster_url, credentials)

    # Stop the QueueListener gracefully before exiting.
    queue_listener.stop()


if __name__ == "__main__":
    main()
