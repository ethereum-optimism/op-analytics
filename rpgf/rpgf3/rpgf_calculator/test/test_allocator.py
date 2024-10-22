# -*- coding: utf-8 -*-
import pytest
import pandas as pd
from core.allocator import ProjectAllocator


# Fixture for the allocator instance
@pytest.fixture
def allocator():
    total_amount = 10000
    min_amount = 1500
    quorum = 2
    return ProjectAllocator(total_amount, min_amount, quorum)


# Fixture for sample data
@pytest.fixture
def sample_data():
    return pd.DataFrame(
        {
            "project_id": [
                "proj1",
                "proj1",
                "proj2",
                "proj2",
                "proj3",
                "proj4",
                "proj4",
            ],
            "voter_address": [
                "addr1",
                "addr2",
                "addr3",
                "addr4",
                "addr5",
                "addr6",
                "addr7",
            ],
            "amount": [2000, 3000, 4000, 5000, 6000, 500, 1000],
        }
    )


def test_calculate_initial_allocation_original(allocator, sample_data):
    allocation = allocator.calculate_initial_allocation(sample_data)

    assert "votes_count" in allocation.columns
    assert "median_amount" in allocation.columns
    assert "is_eligible" in allocation.columns

    assert allocation.loc["proj1", "votes_count"] == 2
    assert allocation.loc["proj3", "is_eligible"] == False
    assert allocation.loc["proj4", "is_eligible"] == False


def test_scale_allocation(allocator, sample_data):
    allocation = allocator.calculate_initial_allocation(sample_data)
    eligible_allocation = allocation[allocation["is_eligible"] == True]
    scaled_allocation = allocator.scale_allocations(
        eligible_allocation, "median_amount"
    )

    assert scaled_allocation.loc["proj1", "median_amount"] == 2500
    assert scaled_allocation["median_amount"].sum() == 7000
    assert round(scaled_allocation.loc["proj2", "scaled_amount"], 2) == 6428.57
