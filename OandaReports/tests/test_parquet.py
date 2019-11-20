#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for csci utils parquet"""

import os
import pandas as pd
from unittest import TestCase


from csci_utils.parquet import convert_parquet
from csci_utils.io import atomic_write


class TestParquet(TestCase):
    @classmethod
    def setUpClass(cls):
        data = {"Name": ["Fido", "Larry", "Scooby Doo", "Mille"],
                "Age": [4, 10, 6, 3]}
        df = pd.DataFrame(data, index=["Terrier", "PON",
                                       "Great Dane", "Birddog"])
        with atomic_write("dogs.xlsx", overwrite=True, mode="w",
                          as_file=False) as f:
            df.to_excel(f, engine="openpyxl")

    def test_write_parquet(self):
        print(os.getcwd())
        if os.path.isfile("dogs.parquet"):
            os.remove("dogs.parquet")
        convert_parquet("dogs.xlsx")
        assert os.path.exists("dogs.parquet")

    @classmethod
    def tearDownClass(cls):
        os.remove("dogs.xlsx")
        os.remove("dogs.parquet")
