import argparse
from luigi import build
from historicrates import GetHistoricRates
from tradinghistory import GetTradingHistory
from reports.volatility import VolatilityReport
from reports.exposure import ExposureReport
from reports.financing import FinancingReport
from reports.opentrades import OpenTradesReport
from reports.netasset import NetAssetReport

#def main():

# Create the parser
my_parser = argparse.ArgumentParser(description='CLI for Oandareports')

# Add the arguments
#my_parser.add_argument('function',
 #                      type=str,
  #                     metavar='function',
   #                    help="""The function you want to run: Alternatives are 'historic' for historic rates,""")
#
my_parser.add_argument('function', action='store', nargs=1, type=str, metavar='function', help="""The function you want to run: Alternatives 
are 'historic' for historic rates, 'trading' for trading history, 'streaming' for streaming rates, 
'volatility' for volatility report, 'exposure' for exposure report, 'financing' for financing report, 'netassets' for net assets report""")

my_parser.add_argument('-i',
                       '--instrument',
                       type=str,
                       metavar='instrument',
                       nargs=1,
                       action='append',
                       help='Instrument; for example EUR_USD or BCO_USD')

my_parser.add_argument('-g',
                       '--granularity',
                       type=str,
                       nargs=1,
                       metavar='granularity',
                       help='Granularity; options are S5, S10, S15, S30 (seconds), M1, M2, M3, M4, M5, M10, M15, M30 (minutes), H1, H2, H3, H4, H6, H8, H12 (hours), D (1 day), W (1 week), M (1 month)')

my_parser.add_argument('-s',
                       '--storage',
                       type=str,
                       nargs=1,
                       default='-s None',
                       metavar='storage',
                       help='Option s3 if data should be stored in AWS S3 location, in addition to local storage')

# Execute the parse_args() method
args = my_parser.parse_args()

#print(my_parser)

if args.function[0] == 'historic':
    instrument = args.instrument[0]
    granularity = args.granularity[0]
    task = build([GetHistoricRates(instrument=instrument, granularity=granularity)], local_scheduler=True)

elif args.function[0] == 'trading':
    s3 = args.storage[0]
    task = build([GetTradingHistory(storage=s3)], local_scheduler=True)

elif args.function[0] == 'streaming':
    import streaming
    instrument = args.instrument[0]
    streaming(instruments=instrument)

elif args.function[0] == 'volatility':
    instrument = args.instrument[0]
    #granularity = args.granularity[0]
    task = build([VolatilityReport(instrument=instrument)], local_scheduler=True)

elif args.function[0] == 'exposure':
    #instrument = args.instrument[0]
    #granularity = args.granularity[0]
    task = build([ExposureReport()], local_scheduler=True)

elif args.function[0] == 'financing':
    #instrument = args.instrument[0]
    #granularity = args.granularity[0]
    task = build([FinancingReport()], local_scheduler=True)

elif args.function[0] == 'opentrades':
    #instrument = args.instrument[0]
    #granularity = args.granularity[0]
    task = build([OpenTradesReport()], local_scheduler=True)

elif args.function[0] == 'netassets':
    #instrument = args.instrument[0]
    #granularity = args.granularity[0]
    task = build([NetAssetReport()], local_scheduler=True)

else:
    print("You need to add a function. Type -h for help.")