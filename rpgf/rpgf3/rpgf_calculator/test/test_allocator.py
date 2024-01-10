# -*- coding: utf-8 -*-
import pytest
import pandas as pd
from core.allocator import ProjectAllocator


# Fixture for the allocator instance
@pytest.fixture
def allocator():
    total_amount = 100000
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


# Fixture for step function
@pytest.fixture
def step_function():
    return {
        1: 1000,
        2: 2000,
        3: 3000,
        4: 4000,
        5: 5000,
        6: 6000,
    }


def test_calculate_initial_allocation_stepby(allocator, sample_data, step_function):
    allocation = allocator.calculate_initial_allocation(sample_data, step_function)

    assert "votes_count" in allocation.columns
    assert "median_amount" in allocation.columns
    assert "step_amount" in allocation.columns
    assert "is_eligible" in allocation.columns

    assert allocation.loc["proj1", "votes_count"] == 2
    assert allocation.loc["proj1", "step_amount"] == 2000
    assert allocation.loc["proj3", "is_eligible"] == False
    assert allocation.loc["proj4", "is_eligible"] == False
    assert allocation[allocation["is_eligible"]]["step_amount"].sum() == 4000


def test_calculate_initial_allocation_original(allocator, sample_data):
    allocation = allocator.calculate_initial_allocation(sample_data)

    assert "votes_count" in allocation.columns
    assert "median_amount" in allocation.columns
    assert "step_amount" in allocation.columns
    assert "is_eligible" in allocation.columns

    assert allocation.loc["proj1", "votes_count"] == 2
    assert allocation.loc["proj1", "step_amount"] == 2500
    assert allocation.loc["proj3", "is_eligible"] == False
    assert allocation.loc["proj4", "is_eligible"] == False
    assert allocation[allocation["is_eligible"]]["step_amount"].sum() == 7000


def test_scale_allocations_stepby(allocator, sample_data, step_function):
    initial_allocation = allocator.calculate_initial_allocation(
        sample_data, step_function
    )
    scaled_allocation = allocator.scale_allocations(
        initial_allocation[initial_allocation["is_eligible"] == True], "step_amount"
    )

    assert "scaled_amount" in scaled_allocation.columns
    assert scaled_allocation["scaled_amount"].sum() == pytest.approx(100000, abs=1e-6)
    assert scaled_allocation.loc["proj1", "scaled_amount"] == pytest.approx(
        50000, abs=1e-6
    )


def test_apply_step_function(allocator, step_function):
    amount = 3000
    votes = 2
    max_amount = allocator._apply_step_function(votes, amount, step_function)

    assert max_amount == min(amount, step_function[votes])
