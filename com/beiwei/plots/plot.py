from io import BytesIO
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib as mpl

def plot_data(df, title, data_field, data_date):
    fig = Figure()
    ax = fig.subplots()
    df.plot(kind='line', x=data_date, y=data_field, ax=ax, title=title)
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_data_w_text(df, title, data_field, data_date, text):
    fig = Figure()
    fig.set_figheight(20)
    fig.set_figwidth(20)
    ax = fig.subplots(2, gridspec_kw={'height_ratios':[2, 1]})
    df.plot(kind='line', x=data_date, y=data_field, ax=ax[0], title=title)
    ax[1].text(0, 0, text, fontsize='x-large')
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_rolling_mean(df, title, data_field, data_date):
    fig = Figure()
    ax = fig.subplots()
    df['rolling_mean'] = df[data_field].rolling(window=12).mean().dropna()
    df.plot(kind='line', x=data_date, y='rolling_mean', ax=ax, color='red', label='rolling mean')
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_rolling_std(df, title, data_field, data_date):
    fig = Figure()
    ax = fig.subplots()
    df['rolling_std'] = df[data_field].rolling(window=12).std().dropna()
    df.plot(kind='line', x=data_date, y='rolling_std', ax=ax, color='red', label='rolling std')
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_seasonal_decompose(df, title, data_field, data_date, text):
    fig = Figure()
    fig.set_figheight(20)
    fig.set_figwidth(20)
    if text is None:
        ax = fig.subplots(4)
    else:
        ax = fig.subplots(5)
    df.plot(kind='line', x=data_date, y=data_field, ax=ax[0], label='Original', title=title)
    df.plot(kind='line', x=data_date, y='trend', ax=ax[1], label='Trend')
    df.plot(kind='line', x=data_date, y='seasonal', ax=ax[2], label='Seasonality')
    df.plot(kind='line', x=data_date, y='residual', ax=ax[3], label='Residuals')
    if text is not None:
        ax[4].text(0, 0, text, fontsize='x-large')
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_eemd(df, imf_size, title, data_date, text):
    fig = Figure()
    fig.set_figheight(2*imf_size)
    fig.set_figwidth(2*imf_size)
    if text is None:
        ax = fig.subplots(imf_size)
    else:
        ax = fig.subplots(imf_size+1)
    for i in range(imf_size):
        index_str = 'imf'+str(i)
        df.plot(kind='line', x=data_date, y=index_str, ax=ax[i], label=index_str)

    if text is not None:
        ax[imf_size].text(0, 0, text, fontsize='x-large')
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_bollingerband(df, title1, title2, date_name, text):
    fig = Figure()
    fig.set_figheight(30)
    fig.set_figwidth(30)
    ax = fig.subplots(4)
    #df.plot(kind='line', x=date_name, y='BBU', ax=ax[0], color='green', label='BBU', style=['--'])
    #df.plot(kind='line', x=date_name, y='BBL', ax=ax[0], color='green', label='BBL', style=['--'])
    #df.plot(kind='line', x=date_name, y='daily', ax=ax[0], color='blue', label='daily')
    df.plot(kind='line', x=date_name, y='BBW', ax=ax[0], color='black', label='BBW')
    #df.plot(kind='line', x=date_name, y='PercentB', ax=ax[2], color='purple', label='%b')
    font = mpl.font_manager.FontProperties(fname='/usr/share/fonts/truetype/arphic-gkai00mp/gkai00mp.ttf')
    ax[0].set_title(title2, fontproperties=font, fontsize='xx-large')

    ax[3].text(0, 0, text, fontsize='x-large')
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()
