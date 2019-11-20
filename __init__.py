from setuptools_scm import get_version

# To set the version
my_version = get_version(
    local_scheme="dirty-tag", write_to="_version.py", fallback_version="0.1.0"
)
