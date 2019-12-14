==============================================================================
OandaReports.  Open source tool for reporting Oanda trading
==============================================================================
:Info: The standardized README file for OandaReports.
:Author: Oyvind Sporck & Diana Liu

:Version: 0.12.1

.. index: README
.. image:: https://travis-ci.com/oeyvindds/oandareports.svg?branch=master
   :target: https://travis-ci.com/oeyvindds/oandareports

.. image:: https://api.codeclimate.com/v1/badges/14f80df17e87c4c3510e/maintainability
    :target: https://codeclimate.com/github/oeyvindds/oandareports/maintainability

.. image:: https://api.codeclimate.com/v1/badges/14f80df17e87c4c3510e/test_coverage
    :target: https://codeclimate.com/github/oeyvindds/oandareports/test_coverage

PURPOSE
-------

INSTALLATION
------------

Your .env file needs the following keys:

- AWS_ACCESS_KEY_ID= Access key for Amazon S3
- AWS_SECRET_ACCESS_KEY= Secret key for Amazon S3
- S3_location = Directory for storing at S3. Example: s3://oanda/
- TOKEN= Token from Oanda
- ACCOUNT_ID= Account id from Oanda
- local_location = Directory for local storage of files. Example: data/
- OandaEnv = Oanda environment: Will be either practice or live

USAGE
-----

NOTES
-----


TROUBLESHOOTING
---------------

Error:   TypeError: unsupported operand type(s) for +: 'NoneType' and 'str'

If the program cannot find a .env file with the needed information, it will give this error. This in particular means that it cannot create a link of a non-existent path