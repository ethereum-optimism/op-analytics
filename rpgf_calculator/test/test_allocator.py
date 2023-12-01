# -*- coding: utf-8 -*-
import pytest
import pandas as pd
from core.utils import ProjectAllocator


def test_calculate_initial_allocation():
    # Initialize ProjectAllocator with dummy values
    allocator = ProjectAllocator(total_amount=10000, min_amount=100, quorum=2)

    # Create a sample DataFrame
    data = {
        "project_id": ["A", "A", "A", "B", "B", "C"],
        "voter_address": ["AA", "AB", "ACA", "AA", "ABAB", "AA"],
        "amount": [100, 200, 250, 300, 400, 500],
    }

    df = pd.DataFrame(data)

    result = allocator.calculate_initial_allocation(df)

    assert result.loc["A", "votes_count"] == 3
    assert result.loc["A", "median_amount"] == 200
    assert result.loc["B", "median_amount"] == 350
    assert result.loc["C", "is_eligible"] == False


def test_scale_allocations():
    allocator = ProjectAllocator(total_amount=6000, min_amount=3000, quorum=3)

    data = {
        "project_id": ["A", "B"],
        "median_amount": [1000, 2000],
        "votes_count": [3, 3],
    }

    initial_allocation = pd.DataFrame(data).set_index("project_id")

    result = allocator.scale_allocations(initial_allocation)

    assert len(result) == 1
    assert not "A" in result.index
    assert result.loc["B", "scaled_amount"] == 4000
