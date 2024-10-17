# -*- coding: utf-8 -*-
from op_datasets.logic.transforms import daily_address_summary


def test_daily_address_summary():
    assert daily_address_summary("2000:2100") == "daily_address_summary(2000:2100)"
