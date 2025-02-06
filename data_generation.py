
import datetime
import random
from typing import Generator

# Generate 500 unique group names
unique_groups = [f'group{i}' for i in range(500)]
random.shuffle(unique_groups)


def package_generator(amount: int, agents: list[dict]) -> Generator[dict, None, None]:
    """Generate a list of random package events."""
    for agent in agents:
        for _ in range(amount):
            yield {
                "agent": {
                    "id": agent.get('agent', {}).get('id', f'agent{random.randint(0, 99)}'),
                    "name": agent.get('agent', {}).get('name', f'Agent{random.randint(0, 99)}'),
                    "groups": agent.get('agent', {}).get('groups', [f'group{random.randint(0, 99)}', f'group{random.randint(0, 99)}']),
                    "type": agent.get('agent', {}).get('type', random.choice(['windows', 'linux', 'macos'])),
                    "version": agent.get('agent', {}).get('version', f'v{random.randint(0, 9)}-stable'),
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
                    "architecture": agent.get('agent', {}).get('host', {}).get('architecture', random.choice(['x86_64', 'arm64'])),
                    'description': f'description{random.randint(0, 9999)}',
                    'installed': __generate_random_date(),
                    'name': f'package{random.randint(0, 9999)}',
                    'path': f'/path/to/package{random.randint(0, 9999)}',
                    'size': random.randint(1000, 100000),
                    'type': random.choice(['deb', 'rpm']),
                    'version': f'{random.randint(0, 9)}.{random.randint(0, 9)}.{random.randint(0, 9)}'
                }
            }


def agents_generator(amount: int, _: list = []) -> Generator[dict, None, None]:
    """Generate a list of random agent events."""
    num_windows = int(0.5 * amount)
    num_macos = int(0.15 * amount)
    num_linux = amount - num_windows - num_macos

    for _ in range(num_windows):
        yield {'agent': __generate_random_agent('windows')}
    for _ in range(num_macos):
        yield {'agent': __generate_random_agent('macos')}
    for _ in range(num_linux):
        yield {'agent': __generate_random_agent('linux')}


def __generate_random_agent(agent_type: str) -> dict:
    """Generate a random agent configuration."""
    agent_id = random.randint(0, 99999)
    return {
        'id': f'agent{agent_id}',
        'name': f'Agent{agent_id}',
        'type': agent_type,
        'version': f'v{random.randint(0, 9)}-stable',
        'status': random.choice(['active', 'inactive']),
        'last_login': __generate_random_date(),
        'groups': __generate_random_groups(agent_type),
        'key': f'key{agent_id}',
        'host': __generate_random_host(agent_type)
    }


def __generate_random_host(agent_type: str) -> dict:
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


def __generate_random_date() -> str:
    """Generate a random date within the last 10 days."""
    start_date = datetime.datetime.now()
    end_date = start_date - datetime.timedelta(days=10)
    random_date = start_date + (end_date - start_date) * random.random()
    return random_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def __generate_random_groups(named_group: str = None) -> list[str]:
    """Return a list of randomly sampled groups."""
    groups = random.sample(unique_groups, 128)
    if named_group:
        groups.pop()
        groups.append(named_group)
    return groups
