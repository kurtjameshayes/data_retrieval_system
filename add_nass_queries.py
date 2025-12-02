#!/usr/bin/env python3
"""
Add USDA NASS QuickStats Queries to MongoDB

This script seeds MongoDB with pre-configured queries that target the
USDA National Agricultural Statistics Service (NASS) QuickStats API. The
queries cover common agricultural insights such as production, planted
acreage, yield, inventory, and price received metrics.

MongoDB URI: mongodb+srv://kurtjhayes_db_user:Rvw6cndMQjWOilXj@cluster0.ngyd1r7.mongodb.net/?appName=Cluster0
Database: data_retrieval_system

Usage:
    python add_nass_queries.py              # Add all queries
    python add_nass_queries.py --list       # List queries to be added
    python add_nass_queries.py --show <id>  # Show query details
"""

import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pymongo import MongoClient
from models.stored_query import StoredQuery

# MongoDB Configuration
MONGO_URI = "mongodb+srv://kurtjhayes_db_user:Rvw6cndMQjWOilXj@cluster0.ngyd1r7.mongodb.net/?appName=Cluster0"
DATABASE_NAME = "data_retrieval_system"

# USDA NASS QuickStats Queries
NASS_QUERIES = [
    {
        "query_id": "nass_corn_production_state_2023",
        "query_name": "US Corn Production by State (2023)",
        "connector_id": "usda_quickstats",
        "description": "State-level corn production totals (bushels) for the 2023 growing season.",
        "parameters": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "PRODUCTION",
            "unit_desc": "BU",
            "agg_level_desc": "STATE",
            "year": "2023"
        },
        "tags": ["nass", "corn", "production", "state", "agriculture"],
        "notes": {
            "filters": "Returns one record per state. Adjust 'year' for other seasons.",
            "source": "USDA NASS QuickStats"
        }
    },
    {
        "query_id": "nass_wheat_yield_state_2022",
        "query_name": "US Wheat Yield by State (2022)",
        "connector_id": "usda_quickstats",
        "description": "Average wheat yield (bushels per acre) for each state in 2022.",
        "parameters": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "YIELD",
            "unit_desc": "BU / ACRE",
            "agg_level_desc": "STATE",
            "year": "2022"
        },
        "tags": ["nass", "wheat", "yield", "state", "production"],
        "notes": {
            "filters": "Include 'class_desc' (e.g., WINTER) to narrow to specific wheat types.",
            "source": "USDA NASS QuickStats"
        }
    },
    {
        "query_id": "nass_soybean_area_planted_state_2023",
        "query_name": "US Soybean Area Planted by State (2023)",
        "connector_id": "usda_quickstats",
        "description": "Planted acreage totals for soybeans, aggregated by state for 2023.",
        "parameters": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "AREA PLANTED",
            "unit_desc": "ACRES",
            "agg_level_desc": "STATE",
            "year": "2023"
        },
        "tags": ["nass", "soybeans", "acreage", "state", "area"],
        "notes": {
            "filters": "Change 'agg_level_desc' to 'COUNTY' for county-level acreage.",
            "source": "USDA NASS QuickStats"
        }
    },
    {
        "query_id": "nass_cattle_inventory_state_2024",
        "query_name": "Cattle Inventory by State (2024)",
        "connector_id": "usda_quickstats",
        "description": "Latest cattle inventory statistics by state for reporting year 2024.",
        "parameters": {
            "commodity_desc": "CATTLE",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "unit_desc": "HEAD",
            "year": "2024"
        },
        "tags": ["nass", "cattle", "inventory", "state", "livestock"],
        "notes": {
            "filters": "Add 'class_desc' (e.g., ALL CATTLE) if you need a narrower category.",
            "source": "USDA NASS QuickStats"
        }
    },
    {
        "query_id": "nass_milk_production_state_2023",
        "query_name": "Milk Production by State (2023)",
        "connector_id": "usda_quickstats",
        "description": "Milk production volume (pounds) by state for 2023, useful for dairy analyses.",
        "parameters": {
            "commodity_desc": "MILK",
            "statisticcat_desc": "PRODUCTION",
            "unit_desc": "LB",
            "agg_level_desc": "STATE",
            "year": "2023"
        },
        "tags": ["nass", "milk", "production", "state", "dairy"],
        "notes": {
            "filters": "Combine with 'month' or 'period' parameters for monthly reporting windows.",
            "source": "USDA NASS QuickStats"
        }
    },
    {
        "query_id": "nass_corn_price_received_iowa_2023",
        "query_name": "Corn Price Received - Iowa (2023)",
        "connector_id": "usda_quickstats",
        "description": "Average price received for corn in Iowa during 2023.",
        "parameters": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "PRICE RECEIVED",
            "state_alpha": "IA",
            "agg_level_desc": "STATE",
            "year": "2023"
        },
        "tags": ["nass", "corn", "price", "iowa", "economics"],
        "notes": {
            "filters": "Set 'state_alpha' to other abbreviations to replicate for different states.",
            "source": "USDA NASS QuickStats"
        }
    },
    {
        "query_id": "nass_cotton_area_harvested_county_tx_2023",
        "query_name": "Texas Cotton Area Harvested by County (2023)",
        "connector_id": "usda_quickstats",
        "description": "County-level harvested acres for cotton in Texas during 2023.",
        "parameters": {
            "commodity_desc": "COTTON",
            "statisticcat_desc": "AREA HARVESTED",
            "state_alpha": "TX",
            "agg_level_desc": "COUNTY",
            "unit_desc": "ACRES",
            "year": "2023"
        },
        "tags": ["nass", "cotton", "county", "texas", "acreage"],
        "notes": {
            "filters": "County names are returned in 'county_name'. Adjust 'state_alpha' for other states.",
            "source": "USDA NASS QuickStats"
        }
    }
]


def check_mongodb():
    """Check if MongoDB is accessible."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        print("\u2713 MongoDB connection successful\n")
        return True
    except Exception as e:
        print(f"\u2717 MongoDB connection failed: {str(e)}")
        print(f"  URI: {MONGO_URI}\n")
        return False


def list_queries():
    """List all NASS queries that will be added."""
    print("=" * 70)
    print("USDA NASS QUERIES TO BE ADDED")
    print("=" * 70 + "\n")

    print(f"Total queries: {len(NASS_QUERIES)}\n")

    categories = {}
    for query in NASS_QUERIES:
        category = query['tags'][1] if len(query['tags']) > 1 else 'other'
        categories.setdefault(category, []).append(query)

    for category, queries in categories.items():
        print(f"\n{category.upper()} Queries ({len(queries)}):")
        print("-" * 70)
        for query in queries:
            print(f"\n  ID: {query['query_id']}")
            print(f"  Name: {query['query_name']}")
            print(f"  Description: {query['description'][:80]}...")
            if 'notes' in query and 'filters' in query['notes']:
                print(f"  Notes: {query['notes']['filters'][:60]}...")

    print("\n" + "=" * 70)
    print(f"Total: {len(NASS_QUERIES)} queries")
    print("=" * 70 + "\n")


def add_queries():
    """Add all NASS queries to MongoDB."""
    print("=" * 70)
    print("ADDING USDA NASS QUERIES TO MONGODB")
    print("=" * 70 + "\n")

    stored_query = StoredQuery()

    results = {
        "added": 0,
        "updated": 0,
        "failed": 0,
        "skipped": 0
    }

    for query_data in NASS_QUERIES:
        query_id = query_data['query_id']

        try:
            existing = stored_query.get_by_id(query_id)

            if existing:
                stored_query.update(query_id, query_data)
                print(f"\u27f3 Updated: {query_id}")
                print(f"  Name: {query_data['query_name']}")
                results["updated"] += 1
            else:
                stored_query.create(query_data)
                print(f"\u2713 Added: {query_id}")
                print(f"  Name: {query_data['query_name']}")
                results["added"] += 1

            print()

        except Exception as e:
            print(f"\u2717 Failed: {query_id}")
            print(f"  Error: {str(e)}\n")
            results["failed"] += 1

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\u2713 Added: {results['added']}")
    print(f"\u27f3 Updated: {results['updated']}")
    print(f"\u2717 Failed: {results['failed']}")
    print("=" * 70 + "\n")

    if results["added"] > 0 or results["updated"] > 0:
        print("Next Steps:")
        print("1. List all queries:")
        print("   python manage_queries.py --list")
        print()
        print("2. Execute a query:")
        print("   python manage_queries.py --execute nass_corn_production_state_2023")
        print()
        print("3. Customize parameters:")
        print("   Use manage_queries.py --update <id> with new NASS filters")
        print()


def show_query_details(query_id):
    """Show detailed information about a specific query."""
    query = next((q for q in NASS_QUERIES if q['query_id'] == query_id), None)

    if not query:
        print(f"Query '{query_id}' not found.\n")
        return

    print("=" * 70)
    print(f"QUERY DETAILS: {query_id}")
    print("=" * 70 + "\n")

    print(f"Name: {query['query_name']}")
    print(f"Connector: {query['connector_id']}")
    print(f"Description: {query['description']}\n")

    print("Parameters:")
    print(json.dumps(query['parameters'], indent=2))
    print()

    if 'notes' in query:
        print("Notes:")
        for key, value in query['notes'].items():
            print(f"  {key}: {value}")
        print()

    print("Tags:", ", ".join(query['tags']))
    print()


def show_usage():
    """Show usage information."""
    print("""
Add USDA NASS QuickStats Queries to MongoDB

This script seeds commonly requested USDA agricultural queries so they can be
managed via manage_queries.py and executed through the query engine.

Usage:
    python add_nass_queries.py              # Add all queries
    python add_nass_queries.py --list       # List queries to be added
    python add_nass_queries.py --show <id>  # Show query details

Categories Covered:
  - Crop production, acreage, and yield
  - Livestock inventory
  - Dairy production
  - Commodity price received
  - County-level harvested acreage

Database:
    URI: mongodb+srv://kurtjhayes_db_user:...@cluster0.ngyd1r7.mongodb.net/
    Database: data_retrieval_system
    Collection: stored_queries
""")


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command in ['-h', '--help', 'help']:
            show_usage()
            return

        elif command in ['-l', '--list', 'list']:
            list_queries()
            return

        elif command in ['-s', '--show', 'show']:
            if len(sys.argv) < 3:
                print("Error: Query ID required")
                print("Usage: python add_nass_queries.py --show <query_id>")
                return
            show_query_details(sys.argv[2])
            return

        else:
            print(f"Unknown command: {command}")
            show_usage()
            return

    print("\n" + "=" * 70)
    print("ADD USDA NASS QUERIES TO MONGODB")
    print("=" * 70 + "\n")

    if not check_mongodb():
        sys.exit(1)

    add_queries()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(0)
