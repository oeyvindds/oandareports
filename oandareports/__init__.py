from luigi.parameter import Parameter
from luigi import Task, ExternalTask
from oandareports.helperfiles.task import TargetOutput, Requires, Requirement
from oandareports.helperfiles.target import ParquetTarget
from dotenv import load_dotenv

load_dotenv()
