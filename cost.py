import json
import logging
from datetime import datetime, timedelta
from typing import Set, Optional

import boto3
from mcp.server.fastmcp import FastMCP

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS Cost Explorer Client Initialization
session = boto3.Session(profile_name='hackmds')
client = session.client('ce')

# Initialize FastMCP server
mcp = FastMCP("cost")

# File path for storing previous services
PREVIOUS_SERVICES_FILE = 'previous_services.json'

def get_cost_and_usage(start_date: str, end_date: str) -> Optional[dict]:
    """Fetch cost and usage data from AWS Cost Explorer."""
    try:
        response = client.get_cost_and_usage(
            TimePeriod={'Start': start_date, 'End': end_date},
            Granularity='MONTHLY',
            Metrics=['BlendedCost'],
            GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
        )
        return response
    except Exception as e:
        logger.error(f"Error fetching cost and usage data: {e}")
        return None

def extract_services(data: dict) -> Set[str]:
    """Extract service names from the Cost Explorer response."""
    services = set()
    try:
        for result in data['ResultsByTime']:
            for group in result['Groups']:
                service_name = group['Keys'][0]
                services.add(service_name)
    except KeyError as e:
        logger.error(f"Error parsing response data: {e}")
    return services

def load_previous_services() -> Set[str]:
    """Load the previously stored services for comparison."""
    try:
        with open(PREVIOUS_SERVICES_FILE, 'r') as f:
            return set(json.load(f))
    except FileNotFoundError:
        logger.info(f"{PREVIOUS_SERVICES_FILE} not found, starting with an empty set.")
        return set()
    except json.JSONDecodeError as e:
        logger.error(f"Error reading JSON data from {PREVIOUS_SERVICES_FILE}: {e}")
        return set()

def save_current_services(services: Set[str]) -> None:
    """Save the current services to a file for future comparison."""
    try:
        with open(PREVIOUS_SERVICES_FILE, 'w') as f:
            json.dump(list(services), f)
    except IOError as e:
        logger.error(f"Error saving services to {PREVIOUS_SERVICES_FILE}: {e}")

def compare_services(last_month_services: Set[str], current_month_services: Set[str]) -> Set[str]:
    """Compare the current and previous services to detect new services."""
    new_services = current_month_services - last_month_services
    return new_services

@mcp.tool()
async def flag_new_services(region: str) -> str:
    """Get and compare new AWS services detected in billing."""
    # Get the current date and calculate the date 6 months ago
    today = datetime.today()
    # AWS Cost Explorer Client Initialization
    session = boto3.Session(profile_name='hackmds')
    client = session.client('ce')
    six_months_ago = today - timedelta(days=180)
    start_date = six_months_ago.strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    # Fetch cost data for the last 6 months
    cost_data = get_cost_and_usage(start_date, end_date)
    if not cost_data:
        return "Error: Could not fetch AWS cost data"

    # Extract services from the fetched data
    current_month_services = extract_services(cost_data)
    if not current_month_services:
        return "No services found in the AWS cost data."

    # Load previously stored services for comparison
    previous_services = load_previous_services()

    # Compare services and detect new ones
    new_services = compare_services(previous_services, current_month_services)

    # Save the updated list of services for future comparison
    save_current_services(current_month_services)

    # Return result
    if new_services:
        logger.info(f"New services detected: {new_services}")
        return f"New AWS services detected: {', '.join(new_services)}"
    else:
        logger.info("No new AWS services detected since the last check.")
        return "No new AWS services detected since last check."


if __name__ == "__main__":
    # Initialize and run the FastMCP server
    mcp.run(transport='stdio')
