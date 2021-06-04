from statsmodels.tsa.stattools import adfuller, kpss
from statsmodels.tsa.seasonal import seasonal_decompose


def adf_test(timeseries):
    dftest = adfuller(timeseries, autolag='AIC')
    response = {'Test Statistic': dftest[0], 'p-value': dftest[1], '#Lags Used': dftest[2],
                'Number of Observations Used': dftest[3]}
    for key, value in dftest[4].items():
        response['Critical Value (%s)' % key] = value
    response['5'] = dftest[5]
    return response


def kpss_test(timeseries):
    kpsstest = kpss(timeseries, regression='c', nlags="auto")
    response = {'Test Statistic': kpsstest[0], 'p-value': kpsstest[1], '#Lags Used': kpsstest[2]}
    for key, value in kpsstest[3].items():
        response['Critical Value (%s)' % key] = value
    return response


def decompose(timeseries):
    decomposition = seasonal_decompose(timeseries, model='additive', period=1)
    trend = decomposition.trend
    seasonal = decomposition.seasonal
    residual = decomposition.resid
    return trend, seasonal, residual

