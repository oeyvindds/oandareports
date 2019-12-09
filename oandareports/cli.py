import argparse
from luigi import build
from historicrates import GetHistoricRates

#def main():

# Create the parser
my_parser = argparse.ArgumentParser(description='CLI for Oandareports')

# Add the arguments
#my_parser.add_argument('function',
 #                      type=str,
  #                     metavar='function',
   #                    help="""The function you want to run: Alternatives are 'historic' for historic rates,""")
#
my_parser.add_argument('function', action='store', nargs=2, help="""The function you want to run: Alternatives are 'historic' for historic rates,""")

my_parser.add_argument('-i',
                       '--instrument',
                       type=str,
                       metavar='instrument',
                       nargs=1,
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
                       nargs=2,
                       metavar='storage',
                       help='Option s3 if data should be stored in AWS S3 location, in addition to local storage')

# Execute the parse_args() method
args = my_parser.parse_args()

print(args)

if args.function[1] == 'historic':
    instrument = args.instrument[0]
    granularity = args.granularity[0]
    task = build([GetHistoricRates(instrument=instrument, granularity=granularity)], local_scheduler=True)


#if __name__ == __main__:
#    main()

