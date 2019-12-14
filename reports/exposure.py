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

    requires = Requires()
    other = Requirement(GetTradingHistory)
    #except:
        #print('kvakk')

    # Set output location
    output = TargetOutput('../'+ os.getenv('local_location') + 'reports/', target_class=ParquetTarget)

    df_list = []

    def create_graph(self, instrument, dsk):
        dsk = dsk[dsk.instrument == instrument]
        dsk['units'] = dsk['units'].astype('int64')
        #dsk['cum_sum'] = dsk['units'].cumsum()
        #dsk['time'] = dd.to_datetime(dsk['time'])
        dsk['time'] = dsk['time'].astype("M8[h]")
        #dsk['time'] = dsk["time"].dt.hour
        #dsk['time'] = dd.to_datetime(dsk['time'])
        #TODO: Add parameter for dato
        #dsk = dsk[dsk['time'] > '2019-10-30']
        #print('kvakk')
        df = dsk.compute()
        df['cum_sum'] = df['units'].cumsum()
        ax = plt.gca()
        sns_plot = sns.lineplot(df['time'], df['cum_sum'], estimator=None)
        sns_plot.set_title('Cummulative exposure in {} units'.format(instrument))
        sns_plot.set_ylabel('Cummulative units')
        sns_plot.set_xlabel('Timeframe')
        #ax.xaxis.set_major_locator(mdates.DayLocator())
        plt.xticks(rotation=45)
        plt.tight_layout()
        #plt.show()
        fig = sns_plot.get_figure()
        name = instrument + '_' + 'exposure.png'
        #TODO: Make directory if not existing
        fig.savefig(os.getenv('local_location') + 'images/' + name)
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

