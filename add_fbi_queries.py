#!/usr/bin/env python3
"""
Add FBI Crime Data Explorer queries to MongoDB.

This utility seeds the `stored_queries` collection with curated FBI Crime Data
Explorer requests. The initial query focuses on nationwide arrests for all
offense categories and mirrors the public API example:
https://api.usa.gov/crime/fbi/cde/arrest/national/all?type=counts&from=01-2023&to=12-2024

Usage:
    python add_fbi_queries.py              # Add all FBI queries
    python add_fbi_queries.py --list       # List the queries that would be added
    python add_fbi_queries.py --show <id>  # Show query configuration details
"""

import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pymongo import MongoClient

from models.stored_query import StoredQuery
from config import Config

# MongoDB Configuration (relies on environment / .env overrides)
MONGO_URI = Config.MONGO_URI
DATABASE_NAME = Config.DATABASE_NAME

FBI_QUERIES = [
    {
        "query_id": "fbi_national_arrests_all_offenses",
        "query_name": "FBI – National Arrests (All Offenses)",
        "connector_id": "fbi_crime",
        "description": (
            "Monthly nationwide arrest totals for all offense categories using "
            "the Crime Data Explorer 'arrest/national/all' endpoint. Returns "
            "both arrest counts and rates per 100,000 people for the supplied "
            "date range."
        ),
        "parameters": {
            "endpoint": "arrest/national/all",
            "type": "counts",
            "from": "01-2023",
            "to": "12-2024"
        },
        "tags": ["fbi", "arrests", "national", "all-offenses"],
        "notes": {
            "endpoint_doc": "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi",
            "sample_request": (
                "https://api.usa.gov/crime/fbi/cde/arrest/national/all"
                "?type=counts&from=01-2023&to=12-2024"
            ),
            "usage": (
                "Override the 'from'/'to' parameters (format MM-YYYY) when "
                "executing via manage_queries or query_fbi.py to adjust the "
                "monthly window."
            )
        }
    }
]


def check_mongodb():
    """Verify MongoDB connectivity before performing any writes."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        print("✓ MongoDB connection successful\n")
        return True
    except Exception as exc:
        print(f"✗ MongoDB connection failed: {exc}")
        print(f"  URI: {MONGO_URI}\n")
        return False


def list_queries():
    """List the FBI queries configured in this script."""
    print("=" * 70)
    print("FBI CRIME DATA QUERIES TO BE ADDED")
    print("=" * 70 + "\n")

    print(f"Total queries: {len(FBI_QUERIES)}\n")

    for query in FBI_QUERIES:
        print(f"ID: {query['query_id']}")
        print(f"  Name: {query['query_name']}")
        print(f"  Description: {query['description']}")
        print(f"  Tags: {', '.join(query.get('tags', [])) or 'none'}")
        print()


def add_queries():
    """Insert or update FBI stored queries."""
    print("=" * 70)
    print("ADDING FBI CRIME DATA QUERIES")
    print("=" * 70 + "\n")

    stored_query = StoredQuery()
    summary = {"added": 0, "updated": 0, "failed": 0}

    for query in FBI_QUERIES:
        query_id = query["query_id"]
        try:
            existing = stored_query.get_by_id(query_id)
            if existing:
                stored_query.update(query_id, query)
                summary["updated"] += 1
                action = "Updated"
            else:
                stored_query.create(query)
                summary["added"] += 1
                action = "Added"

            print(f"{action}: {query_id}")
            print(f"  Name: {query['query_name']}\n")
        except Exception as exc:
            summary["failed"] += 1
            print(f"Failed: {query_id}")
            print(f"  Error: {exc}\n")

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"✓ Added: {summary['added']}")
    print(f"⟳ Updated: {summary['updated']}")
    print(f"✗ Failed: {summary['failed']}")
    print("=" * 70 + "\n")


def show_query_details(query_id: str):
    """Show the configuration for a specific query in this script."""
    query = next((item for item in FBI_QUERIES if item["query_id"] == query_id), None)

    if not query:
        print(f"Query '{query_id}' not found.\n")
        return

    print("=" * 70)
    print(f"QUERY DETAILS: {query_id}")
    print("=" * 70 + "\n")
    print(json.dumps(query, indent=2))
    print()


def show_usage():
    """Display usage instructions."""
    print("""
Add FBI Crime Data Explorer Queries to MongoDB

Usage:
    python add_fbi_queries.py              # Add/Update all queries
    python add_fbi_queries.py --list       # List configured queries
    python add_fbi_queries.py --show <id>  # Show specific query payload

Example:
    python add_fbi_queries.py --show fbi_national_arrests_all_offenses
""")


def main():
    """Main CLI entry point."""
    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command in {"-h", "--help", "help"}:
            show_usage()
            return

        if command in {"-l", "--list", "list"}:
            list_queries()
            return

        if command in {"-s", "--show", "show"}:
            if len(sys.argv) < 3:
                print("Error: Query ID required\n")
                return
            show_query_details(sys.argv[2])
            return

        print(f"Unknown command: {command}\n")
        show_usage()
        return

    print("\n" + "=" * 70)
    print("ADD FBI CRIME DATA QUERIES TO MONGODB")
    print("=" * 70 + "\n")

    if not check_mongodb():
        sys.exit(1)

    add_queries()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(0)
