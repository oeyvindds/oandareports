==============================================================================
OandaReports.  Open source tool for reporting Oanda trading
==============================================================================
:Info: The standardized README file for OandaReports.
:Author: Oyvind Sporck & Diana Liu

:Version: 0.13.0

.. index: README
.. image:: https://travis-ci.com/oeyvindds/oandareports.svg?branch=master
   :target: https://travis-ci.com/oeyvindds/oandareports

.. image:: https://api.codeclimate.com/v1/badges/14f80df17e87c4c3510e/maintainability
    :target: https://codeclimate.com/github/oeyvindds/oandareports/maintainability

.. image:: https://api.codeclimate.com/v1/badges/14f80df17e87c4c3510e/test_coverage
    :target: https://codeclimate.com/github/oeyvindds/oandareports/test_coverage

PURPOSE
-------
Oanda (www.oanda.com) is one of the largest online brokers for foreign exchange and commodities for retail clients, as well as for corporate foreign exchange payments.

Oanda does provide an API to its services ( https://developer.oanda.com ) where the customers may directly perform trades, gather information and so forth thru a number of languages including Python.

Oanda provide their clients with a number of tools to give the investor an overview over their positions, risk, exposure and so forth. However, these tools are aggregated, as in monthly statements, generalized as in not specific to the individual trader or are provided thru third parties such as Chasing Returns ( https://chasingreturns.com ). The latter is geared towards retail traders with manually performed trades. Hence, the tools crash or timeout when exposed to accounts with a large number of trades.

A better understanding of ones trading pattern, and exposure levels are warranted for being successful as a trader. For example, a trade consisting of positions in EUR / USD, Brent Crude Oil and gold might seem like a diversified portfolio. However, especially for a non-US investor, they all give a large exposure to the USD. Even a portfolio with 50 different currencies have a tendency to be overly linked to the USD, just because it is so dominant. However, for an investor it is vital to understand how the exposure is, and to being able to balance it as desired.

This project is attempting to give the investor a better understanding of the trading history and pattern.

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

To use this project, you need an Oanda account. It may use both a 'live' and a 'practice' account. We strongly advise you to try out first with a practice account. If you want to utilize AWS s3, you need a AWS key / secret key.

The project use pipenv for a repeatable environment.

So the steps to get this up and running is as follows:

Clone project from https://github.com/oeyvindds/oandareports.git

Change into the local directory where you have downloaded this

Run 'pipenv install'

Run the command 'pipenv run python cli.py to get to the CLI-interface.

There you have a number of choices for what functionality you want. However, remember to create a .env file with the above mentioned information to get it to work. Without the .env there is not much you can do as you do not have access to Oandas api.

The cli.py is to be used in the following way:

'cli.py function -i instrument -g granularity -s storage'

where function is one of the following:
- report
- historic
- trading
- stream
- volatility
- exposure
- financing
- netassets
- correlation
- automated

Instrument can be any of the tickers provided by Oanda, such as 'BCO_USD' og 'USD_NOK'

Granularity is any of the time-frames provided by the API. For example S5 for 5 seconds or M15 for 15 minutes

Storage is 's3' if you want automatic backup to AWS S3 (Remember to update the .env)

'cli.py -h' will provide you with more information in regards to the choices above, and what they actually provide.

And one last thing; the - report function creates a pdf-report of all the graphs in the images-folder. However, you need to use some of the other functions to generate contents. Otherways you will end up with an empty pdf.

NOTES
-----

This project utilizes the following tools and technologies:

- Oanda APi https://developer.oanda.com
- Oanda-api-v20 https://github.com/hootnot/oanda-api-v20
- Luigi https://luigi.readthedocs.io/en/stable/#
- Dask https://dask.readthedocs.io/en/latest/
- Pandas https://pandas.pydata.org
- Seaborn https://seaborn.pydata.org
- Django https://www.djangoproject.com
- Heroku https://www.heroku.com

In addition a wide range support libraries

TROUBLESHOOTING
---------------

Error:   TypeError: unsupported operand type(s) for +: 'NoneType' and 'str'

If the program cannot find a .env file with the needed information, it will give this error. This in particular means that it cannot create a link of a non-existent path
