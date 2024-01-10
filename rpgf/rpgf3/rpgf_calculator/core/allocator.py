# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from core.utils import get_logger

from typing import Optional, Dict


class ProjectAllocator:
    def __init__(self, total_amount: float, min_amount: float, quorum: int) -> None:
        """
        Initialize the ProjectAllocator.
        """
        self.total_amount = total_amount
        self.min_amount = min_amount
        self.quorum = quorum

    def _apply_step_function(
        self, votes: int, amount: float, step_function: Dict[int, int]
    ) -> float:
        max_amount = step_function.get(
            votes, step_function.get(max(step_function.keys()))
        )
        return min(amount, max_amount)

    def calculate_initial_allocation(
        self, df: pd.DataFrame, step_function: Optional[Dict[int, int]] = None
    ) -> pd.DataFrame:
        """
        Calculate the raw allocation amount of each project.
        """
        # get the number of votes and median amount for each project
        df["valid_vote"] = np.where(df["amount"].notna(), df["voter_address"], np.nan)

        project_allocation = df.groupby("project_id").agg(
            votes_count=("valid_vote", "count"), median_amount=("amount", "median")
        )

        if step_function:
            # apply _apply_step_function to median_amount where is_eligible is True
            project_allocation["step_amount"] = project_allocation.apply(
                lambda row: self._apply_step_function(
                    row["votes_count"], row["median_amount"], step_function
                ),
                axis=1,
            )
        else:
            # if no step function is provided, use the median amount as the step amount
            project_allocation["step_amount"] = project_allocation["median_amount"]

        # if the number of votes is less than the quorum, the project is not eligible
        project_allocation["is_eligible"] = (
            project_allocation["votes_count"] >= self.quorum
        ) & (project_allocation["step_amount"] >= self.min_amount)

        return project_allocation.sort_values("step_amount", ascending=False)

    def scale_allocations(self, df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        """
        Scale the allocations based on predefined step function.
        """
        log = get_logger()

        log.info("Check - If all are eligible: " + df["is_eligible"].all().astype(str))

        amount_eligible = df[col_name].sum()
        scale_factor = self.total_amount / amount_eligible

        log.info("Check - Original Amount Eligible: " + str(amount_eligible))
        log.info("Check - Scale Factor: " + str(scale_factor))

        df["scaled_amount"] = df[col_name] * scale_factor

        log.info("Check - New Amount Eligible: " + str(df["scaled_amount"].sum()))

        return df
