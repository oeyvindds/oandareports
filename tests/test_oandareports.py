import os

from unittest import TestCase
from unittest.mock import patch, MagicMock
from luigi import build, ExternalTask, format, Parameter, Task
from luigi.parameter import MissingParameterException
from luigi.contrib.s3 import S3Target
from luigi import LocalTarget
from luigi.execution_summary import LuigiStatusCode
from luigi.mock import MockTarget

import argparse
from luigi import build
from tools.historicrates import GetHistoricRates
from tools.tradinghistory import GetTradingHistory
from tools.create_pdf import PdfReport
from reports.volatility import VolatilityReport
from reports.exposure import ExposureReport
from reports.financing import FinancingReport
from reports.opentrades import OpenTradesReport
from reports.netasset import NetAssetReport
from reports.correlation import CorrelationReport

def build_func(func, **kwargs):
    return build([func(**kwargs)],
                 local_scheduler=True,
                 detailed_summary=True
                 ).status

class TasksDataTests(TestCase):
    def test_GetHistoricRates(self):
        """Ensure GetHistoricRates works correctly and as expected"""
        with self.assertRaises(MissingParameterException):
            build_func(GetHistoricRates, instrument='EUR_USD')

        with patch('luigi.LocalTarget.exists', MagicMock(return_value=True)):
            self.assertEqual(build_func(GetHistoricRates, instrument='EUR_USD', granularity='S5'), LuigiStatusCode.SUCCESS)

    def test_GetTradingHistory(self):
        """Ensure GetTradingHistory works correctly and as expected"""
        #with self.assertRaises(MissingParameterException):
            #build_func(GetTradingHistory, instrument='EUR_USD')

        with patch('luigi.LocalTarget.exists', MagicMock(return_value=True)):
            self.assertEqual(build_func(GetTradingHistory), LuigiStatusCode.SUCCESS)

    def test_VolatilityReport(self):
        """Ensure GetTradingHistory works correctly and as expected"""
        with self.assertRaises(MissingParameterException):
            build_func(VolatilityReport)

        with patch('luigi.LocalTarget.exists', MagicMock(return_value=True)):
            self.assertEqual(build_func(GetTradingHistory), LuigiStatusCode.SUCCESS)