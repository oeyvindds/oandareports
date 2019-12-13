#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pset_5` package."""

from unittest import TestCase
# import pset_5
# from pset_5.tasks import CleanedReviews, ByDecade, ByStars, CSVTarget, YelpReviews
from csci_utils.luigi.dask.target import ParquetTarget
import re
from datetime import datetime
import dask.dataframe as dd
from luigi import build
import pandas as pd
from tempfile import TemporaryDirectory
import os
import numpy as np


# class Testpset_5(TestCase):
#     """Tests for `pset_5` package."""
#
#     def is_version(self, v):
#         if isinstance(v, str):
#             return bool(re.match(r"^\d+.\d+.", v))
#         else:
#             return False
#
#     def test_pset_version(self):
#         """Check that version of Pset can be obtained"""
#         assert self.is_version(pset_5.__version__)


class TestLuigiTasks(TestCase):
    """Tests for Luigi Tasks in Pset 5."""

    def setUp(self):
        """Set up a clean and dirty version of a test dataframe"""

        self.npartitions = 2

        # clean dataframe
        self.clean_frame = pd.DataFrame(
            {
                "stars": [1, 1, 2, 2, 3, 3, 4, 5],
                "review_id": [
                    "a" * 22,
                    "b" * 22,
                    "c" * 22,
                    "d" * 22,
                    "e" * 22,
                    "f" * 22,
                    "g" * 22,
                    "h" * 22,
                ],
                "user_id": ["fake_id"] * 8,
                "text": ["a", "bbb", "c", "ddd", "ee", "ffff", "gg", "hhhh"],
                "date": [
                    datetime(2001, 10, 1),
                    datetime(2001, 10, 2),
                    datetime(2001, 10, 3),
                    datetime(2001, 10, 4),
                    datetime(2011, 10, 1),
                    datetime(2011, 10, 2),
                    datetime(2011, 10, 3),
                    datetime(2011, 10, 4),
                ],
                "funny": [1] * 8,
                "useful": [1] * 8,
                "cool": [1] * 8,
            }
        )
        # convert to dask
        self.clean_frame_dd = dd.from_pandas(
            self.clean_frame, npartitions=self.npartitions
        ).set_index("review_id")

        # add 2 dirty lines to clean dataframe
        self.dirty_frame = pd.DataFrame(
            {
                "stars": [1, 1, 2, 2, 3, 3, 4, 5, np.NaN, np.NaN],
                "review_id": [
                    "a" * 22,
                    "b" * 22,
                    "c" * 22,
                    "d" * 22,
                    "e" * 22,
                    "f" * 22,
                    "g" * 22,
                    "h" * 22,
                    "i",
                    "j" * 22,
                ],
                "user_id": ["fake_id"] * 9 + [""],
                "text": ["a", "bbb", "c", "ddd", "ee", "ffff", "gg", "hhhh", "", ""],
                "date": [
                    datetime(2001, 10, 1),
                    datetime(2001, 10, 2),
                    datetime(2001, 10, 3),
                    datetime(2001, 10, 4),
                    datetime(2011, 10, 1),
                    datetime(2011, 10, 2),
                    datetime(2011, 10, 3),
                    datetime(2011, 10, 4),
                    datetime(2011, 10, 5),
                    datetime(2011, 10, 6),
                ],
                "funny": [1] * 10,
                "useful": [1] * 10,
                "cool": [1] * 10,
            }
        )
        # convert to dask
        self.dirty_frame_dd = dd.from_pandas(
            self.dirty_frame, npartitions=self.npartitions
        ).set_index("review_id")

        # expected aggregation summaries for the cleaned data frame
        self.expected_bydecade = [2, 3]
        self.expected_bystars = [2, 2, 3, 2, 4]

        self.tmpdir = ""

    def reset_paths(self):
        """function to redirect the output paths of the Tasks to temporary directory"""

        # write test dask frame to file
        self.dirty_frame_dd.to_csv(
            os.path.join(self.tmpdir, "yelp_data", "yelp_subset_*.csv")
        )

        # set output paths
        YelpReviews.S3PATH = os.path.join(self.tmpdir, "yelp_data/")
        CleanedReviews.output = lambda _: ParquetTarget(
            os.path.join(self.tmpdir, "CleanedReviews.parquet/")
        )
        ByDecade.output = lambda _: ParquetTarget(
            os.path.join(self.tmpdir, "ByDecade.parquet/")
        )
        ByStars.output = lambda _: ParquetTarget(
            os.path.join(self.tmpdir, "ByStars.parquet/")
        )

    def testCleanedReviews(self):

        with TemporaryDirectory() as self.tmpdir:

            self.reset_paths()

            # build Luigi Tasks
            build([CleanedReviews(subset=False)], local_scheduler=True)

            # read back result
            outframe_dd = dd.read_parquet(
                os.path.join(self.tmpdir, "CleanedReviews.parquet/")
            )

            # check that processed dirty frame is equal to expected clean frame
            self.assertEqual(
                pd.testing.assert_frame_equal(
                    outframe_dd.compute(), self.clean_frame_dd.compute()
                ),
                None,
            )

    def testByDecade(self):
        """test summary aggregation by decade"""
        with TemporaryDirectory() as self.tmpdir:

            self.reset_paths()

            build(
                [CleanedReviews(subset=False), ByDecade(subset=False)],
                local_scheduler=True,
            )

            # read Summary back from file
            outframe_dd = dd.read_parquet(
                os.path.join(self.tmpdir, "ByDecade.parquet/")
            )

            # check that expected summary result is achieved
            self.assertEqual(
                outframe_dd.compute().text.tolist(), self.expected_bydecade
            )

    def testByStars(self):
        """test summary aggregation by stars"""

        with TemporaryDirectory() as self.tmpdir:

            self.reset_paths()

            build(
                [CleanedReviews(subset=False), ByStars(subset=False)],
                local_scheduler=True,
            )

            outframe_dd = dd.read_parquet(os.path.join(self.tmpdir, "ByStars.parquet/"))
            print(outframe_dd.compute().text.tolist())

            self.assertEqual(outframe_dd.compute().text.tolist(), self.expected_bystars)
