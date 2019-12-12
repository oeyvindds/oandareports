==============================================================================
OandaReports.  Open source tool for reporting Oanda trading
==============================================================================
:Info: The standardized README file for OandaReports.
:Author: Oyvind Sporck & Diana Liu

:Version: 0.1.0

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

- AWS_ACCESS_KEY_ID= For Amazon S3
- AWS_SECRET_ACCESS_KEY= For Amazon S3
- S3_location = Directory for storing at S3. Example: s3://oanda/
- TOKEN= Token from Oanda
- ACCOUNT_ID= Account id from Oanda
- local_location = Directory for local storage of files. Example: data/
- OandaEnv = Oanda environment. Will be either practice or live

USAGE
-----

NOTES
-----