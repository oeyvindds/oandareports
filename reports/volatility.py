import os
import seaborn as sns
import matplotlib.pylab as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates
import dask.dataframe as dd
from luigi import Task
from luigi import build
from luigi.parameter import Parameter
from luigi import Task, ExternalTask
from helperfiles.task import TargetOutput, Requires, Requirement
from helperfiles.target import ParquetTarget
from tools.historicrates import GetHistoricRates

class VolatilityReport(Task):
    help = "Will create volatility-report for specified instrument"

    instrument = Parameter()

    def requires(self):
        task = build([GetHistoricRates(instrument=self.instrument, granularity='D')], local_scheduler=True)
        task = build([GetHistoricRates(instrument=self.instrument, granularity='W')], local_scheduler=True)


    def add_arguments(self, parser):
        # The instrument it should create report for
        parser.add_argument('--instrument', nargs='2', type=str)

    def analyze(self, granularity):
        ddf = dd.read_parquet(os.getenv('local_location') + 'rates/' + self.instrument + '_' + granularity + '/' + 'part.*.parquet')
        ddf = ddf.astype({'complete':'bool', 'volume':'int64', 'o':'float64', 'h':'float64', 'l':'float64', 'c':'float64' })
        ddf = ddf[ddf['complete'] == True]
        ddf['time'] = dd.to_datetime(ddf['time'])
        ddf = ddf.set_index('time')
        ddf['vol'] = ddf['h'] - ddf['l']
        ddf['mov'] = ddf['c'] - ddf['o']
        pdf = ddf.compute()
        ax = sns.jointplot(pdf['mov'], pdf['vol'], alpha=0.2)
        ax.set_axis_labels('Low to high', 'Open to close', fontsize=14)
        if granularity == 'D':
            plt.title('Daily volatility {}'.format(self.instrument), fontsize=14)
            name = 'daily_volatility_{}.png'.format(self.instrument)
        else:
            plt.title('Weekly volatility {}'.format(self.instrument), fontsize=14)
            name = 'weekly_volatility_{}.png'.format(self.instrument)
        plt.tight_layout()
        try:
            ax.savefig(os.getenv('local_location') + 'images/' + name)
        except:
            os.mkdir(os.getenv('local_location') + 'images/')
            ax.savefig(os.getenv('local_location') + 'images/' + name)


    def run(self, *args, **options):
        day = self.analyze('D')
        week = self.analyze('W')
