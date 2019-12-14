# Tests for the csci_utils are included in the cookiecutter package
# if csci_utils is included. It is obviously tested in the library itself,
# but as mentioned in lecture 2; do also test "other peoples" libraries.
# The example in lecture 2 was Numpy. But, as we "pretend" csci_util is
# a third party library, I have included the tests here
# All tests execute without fails in a newly generated project
# installed with csci_utils
# Except for test_sanity, the rest are removed if csci_utils
# are not installed. This is done by post_gen_project.py