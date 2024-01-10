# -*- coding: utf-8 -*-
import logging
import json
import pandas as pd
import random
import string
from datetime import datetime, timedelta
import numpy as np


def get_logger() -> logging.Logger:
    """
    Get logger instance.
    """
    level = logging.INFO
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    logger.propagate = False
    logger.handlers = []
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def safe_json_loads(s):
    """
    Safely loads a JSON string, replacing single quotes with double quotes, and returns a JSON string.
    """
    try:
        if isinstance(s, str):
            # Convert to valid JSON format and parse
            parsed = json.loads(s.replace("'", '"'))
            return json.dumps(parsed)  # Convert back to JSON string
        else:
            return json.dumps(s)  # Return the JSON string representation of s
    except json.JSONDecodeError:
        # Handle or log the error if needed
        return json.dumps([])  # Return an empty JSON array as a string


def expand_json(row, idx):
    """
    Normalize and Expand the JSON-like strings or already parsed JSON.
    """
    if isinstance(row, str):
        parsed_row = json.loads(row)
    elif isinstance(row, list):
        parsed_row = row
    else:
        parsed_row = []

    expanded = pd.json_normalize(parsed_row)
    expanded["original_index"] = idx
    return expanded


class DummyDataGenerator:
    def __init__(
        self,
        num_rows: int,
        max_project_in_ballot: int,
        max_votes: int,
        start_date: str,
        end_date: str,
        seed: int = 42,
    ) -> None:
        """
        Initialize the Dummy Data Generator.
        """
        self.num_rows = num_rows
        self.max_project_in_ballot = max_project_in_ballot
        self.max_votes = max_votes
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.seed = seed

    def _generate_random_address(self):
        """Generate a random address-like string."""
        return "".join(random.choices(string.ascii_letters + string.digits, k=10))

    def _generate_random_datetime(self, start, end):
        """Generate a random datetime between 'start' and 'end'."""
        return start + timedelta(
            seconds=random.randint(0, int((end - start).total_seconds()))
        )

    def _generate_votes(self):
        """Generate a random votes array."""
        num_votes = random.randint(1, self.max_votes)
        projects = random.sample(range(self.max_project_in_ballot), num_votes)
        return [
            {
                "amount": str(random.randint(1000, 5000000)),
                "projectId": f"proj{proj_id}",
            }
            for proj_id in projects
        ]

    def generate_dummy_data(self):
        """Generate dummy data."""
        data = []

        for _ in range(self.num_rows):
            has_published = random.choice([True, False])
            has_voted = True if has_published else random.choice([True, False])
            created_at = self._generate_random_datetime(self.start_date, self.end_date)
            updated_at = self._generate_random_datetime(created_at, self.end_date)
            published_at = (
                self._generate_random_datetime(updated_at, self.end_date)
                if has_published
                else None
            )

            votes = self._generate_votes() if has_published else []
            projects_in_ballot = len(votes)

            row = {
                "Address": self._generate_random_address(),
                "Has voted": has_voted,
                "Has published": has_published,
                "Published at": published_at,
                "Created at": created_at,
                "Updated at": updated_at,
                "Projects in ballot": projects_in_ballot,
                "Signature": self._generate_random_address() if has_published else None,
                "Votes": votes,
            }
            data.append(row)

        return pd.DataFrame(data)
