from setuptools_scm import get_version
import os

"""This file adds git versioning to the newly created project and
adds the version to a _version.py file.

To have it added to GitHub (and thereafter Travis), you may use the following:

        cd oandareports
        git init
        git add --all
        git commit -m "Add initial project skeleton."
        git tag v0.1.0
        git remote add origin git@github.com:repo_username/
            oandareports.git
        git push -u origin master v0.1.0

It may be automatically added by uncommenting below: """

# os.system("cd oandareports")
os.system("git init")
# os.system("git add --all")
# os.system("git commit -m "Add initial project skeleton.")
# os.system("git tag v0.1.0"
# os.system("git remote add origin git@github.com:repo_username/
# /oandareports.git")
# os.system("git push -u origin master v0.1.0")

my_version = get_version(
    local_scheme="dirty-tag", write_to="_version.py", fallback_version="0.1.0"
)
