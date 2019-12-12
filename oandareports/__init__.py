#from luigi.parameter import Parameter
#from luigi import Task, ExternalTask
#from oandareports import historicrates, streaming, tradinghistory
#from oandareports import examples, helperfiles, reports, tests
#from oandareports.helperfiles.task import TargetOutput, Requires, Requirement
#from oandareports.helperfiles.target import ParquetTarget
#import oandareports
from dotenv import load_dotenv

load_dotenv()

from setuptools_scm import get_version

# To set the version
#my_version = get_version(
#    local_scheme="dirty-tag", write_to="_version.py", fallback_version="0.1.0"
#)