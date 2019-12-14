import os
import seaborn as sns
import matplotlib.pylab as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates
import dask.dataframe as dd
from luigi import Task, build
from luigi.parameter import Parameter
from luigi import Task, ExternalTask
from helperfiles.task import TargetOutput, Requires, Requirement
from pandas.plotting import register_matplotlib_converters
from tools.tradinghistory import GetTradingHistory

register_matplotlib_converters()

class FinancingReport(Task):

    requires = Requires()
    other = Requirement(GetTradingHistory)

    # Set output location
    output = TargetOutput(os.getenv('local_location') + '/image')

    df_list = []

    def create_graph(self, df):
        for i in ['accountBalance','financing']:
            ax = plt.gca()
            sns_plot = sns.lineplot(df.index, df[i])
            sns_plot.set_title('Running {}'.format(i))
            sns_plot.set_ylabel('USD')
            plt.xticks(rotation=45)
            plt.tight_layout()
            fig = sns_plot.get_figure()
            if not os.path.exists(os.getenv("local_location") + "images/"):
                os.makedirs(os.getenv("local_location") + "images/")
            fig.savefig(os.getenv('local_location') + 'images/' + '{}.png'.format(i))
            fig.clf()

    def calculate(self, dsk):
        dsk['time'] = dsk['time'].astype("M8[D]")
        dsk = dsk.set_index('time')
        dsk = dsk[dsk['type'].isin(['DAILY_FINANCING'])]
        dsk = dsk[['amount', 'accountBalance', 'financing']]
        dsk['financing'] = dsk['financing'].fillna(0.0)
        dsk['financing'] = dsk['financing'].astype('float64')
        dsk['accountBalance'] = dsk['accountBalance'].astype('float64')
        df = dsk.compute()
        df['financing'] = df['financing'].cumsum(axis=0)
        return df



    def run(self):

        dsk = dd.read_parquet(os.getenv('local_location') + 'trading_history/*.parquet')
        # dsk['time'] = dsk['time'].astype("M8[D]")
        # dsk = dsk.set_index('time')
        # dsk = dsk[dsk['type'].isin(['DAILY_FINANCING'])]
        # dsk = dsk[['amount', 'accountBalance', 'financing']]
        # dsk['financing'] = dsk['financing'].fillna(0.0)
        # dsk['financing'] = dsk['financing'].astype('float64')
        # dsk['accountBalance'] = dsk['accountBalance'].astype('float64')
        # df = dsk.compute()
        # df['financing'] = df['financing'].cumsum(axis=0)
        df = self.calculate(dsk)
        self.create_graph(df)
