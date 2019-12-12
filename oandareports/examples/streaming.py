
"""For streaming of rates.

Example use: streaming.py --inst EUR_USD --inst EUR_JPY

:arg --inst: One or more tickers
:return : json-stream with price information
"""
import os
import argparse
import json
from oandapyV20 import API
from oandapyV20.endpoints.pricing import PricingStream

class Streaming:

    # Get credentials from .env
    access_token=os.getenv('TOKEN')
    accountID=os.getenv('ACCOUNT_ID')

    # Parser for tickers
    parser = argparse.ArgumentParser(prog='streaming')
    parser.add_argument('function', type=str, action='store', nargs=1)
    parser.add_argument('-i', '--instruments', type=str, action='append')
    args = parser.parse_args()

    # Generate for the api-calls
    api = API(access_token=access_token,
              environment=os.getenv('OandaEnv'),
              request_params={})

    r = PricingStream(accountID=accountID,
                      params={"instruments": ",".join(args.instruments)})

    # Continious streaming of rates
    while True:
        try:
            for R in api.request\
                        (r):
                R = json.dumps(R)
                print(R)

        except Exception as e:
            print('Error occured: {}\n'.format(e))
            break
