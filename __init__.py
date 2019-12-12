from setuptools_scm import get_version
#import oandareports
#from oandareports import examples
#from oandareports import helperfiles
#from oandareports import tests
#from oandareports import tools
#from oandareports import reports

from dotenv import load_dotenv

# To set the version
my_version = get_version(
    local_scheme="dirty-tag", write_to="_version.py", fallback_version="0.1.0"
)

load_dotenv()