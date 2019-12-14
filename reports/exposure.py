import os
import seaborn as sns
import matplotlib.pylab as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates
import dask.dataframe as dd
from luigi import Task
from luigi.parameter import Parameter
from luigi import Task, ExternalTask
from helperfiles.task import TargetOutput, Requires, Requirement
from helperfiles.target import ParquetTarget
from tools.tradinghistory import GetTradingHistory

#class GetHistory(Task):
 #   output = TargetOutput('../'+ os.getenv('local_location'), target_class=ParquetTarget)

class ExposureReport(Task):
    def return_env(value):
        value = os.getenv(value)
        if value == None:
            value = 'not_availiable'
        return value

    requires = Requires()
    other = Requirement(GetTradingHistory)

    # Set output location
    output = TargetOutput('../'+ return_env('local_location') + 'reports/', target_class=ParquetTarget)

    df_list = []

    def calculate(self, dsk):
        dsk['units'] = dsk['units'].astype('int64')
        dsk['time'] = dsk['time'].astype("M8[h]")
        df = dsk.compute()
        df['cum_sum'] = df['units'].cumsum()
        return df


    def create_graph(self, instrument, dsk):
        dsk = dsk[dsk.instrument == instrument]
        df = self.calculate(dsk)
        # dsk['units'] = dsk['units'].astype('int64')
        # dsk['time'] = dsk['time'].astype("M8[h]")
        # df = dsk.compute()
        # df['cum_sum'] = df['units'].cumsum()
        ax = plt.gca()
        sns_plot = sns.lineplot(df['time'], df['cum_sum'], estimator=None)
        sns_plot.set_title('Cummulative exposure in {} units'.format(instrument))
        sns_plot.set_ylabel('Cummulative units')
        sns_plot.set_xlabel('Timeframe')
        plt.xticks(rotation=45)
        plt.tight_layout()
        fig = sns_plot.get_figure()
        name = instrument + '_' + 'exposure.png'
        if not os.path.exists(self.return_env('local_location') + 'images/'):
            os.makedirs(self.return_env('local_location') + 'images/')
        fig.savefig(self.return_env('local_location') + 'images/' + name)
        fig.clf()


    def run(self):
        input_target = next(iter(self.input().items()))[1]

        dsk = input_target.read()

        dsk = dsk.drop(columns=['homeCurrency', 'accountUserID','batchID' ,'orderID','userID','accountID','tradeID','pl','requestID','gainQuoteHomeConversionFactor','requestedUnits','fundingReason', 'timeInForce','positionFill','triggerCondition','partialFill','alias',
                                'accountNumber','divisionID','siteID','commission','rejectReason','accountFinancingMode','financing','marginRate','guaranteedExecutionFee',
                                'lossQuoteHomeConversionFactor', 'halfSpreadCost','fullVWAP'], errors='ignore')
        dsk = dsk[dsk.type.isin(['ORDER_FILL'])]

        for i in dsk['instrument'].unique().compute():
            print(i)
            self.create_graph(i, dsk)

