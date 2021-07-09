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
    fig.set_figheight(20)
    fig.set_figwidth(15)
    ax = fig.subplots(3)

    df.plot(kind='line', x=date_name, y='BBU', ax=ax[0], color='green', label='BBU', style=['--'])
    df.plot(kind='line', x=date_name, y='BBL', ax=ax[0], color='green', label='BBL', style=['--'])
    df.plot(kind='line', x=date_name, y='SMA', ax=ax[0], color='green', label='SMA', style=['--'])
    df.plot(kind='line', x=date_name, y='Daily', ax=ax[0], color='blue', label=u'Daily')
    df.plot(kind='line', x=date_name, y='BBW', ax=ax[1], color='black', label='BBW')
    font = mpl.font_manager.FontProperties(fname='/usr/share/fonts/truetype/arphic-gkai00mp/gkai00mp.ttf')
    ax[0].set_title(title1, fontproperties=font, fontsize='xx-large')
    ax[0].legend(prop=font)
    ax[1].set_title(title2, fontproperties=font, fontsize='xx-large')
    ax[2].text(0, 0, text, fontsize='x-large')
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_color(values):
    color = []
    for value in values:
        if value == 1:
            color.append('red')
        elif value == 2:
            color.append('orange')
        elif value == 3:
            color.append('aqua')
        elif value == 4:
            color.append('blue')
        else:
            color.append('gray')
    return color


def iex_plot_bbtrend(df, title1, title2, date_name):
    fig = Figure()
    fig.set_figheight(15)
    fig.set_figwidth(15)
    ax = fig.subplots(2)
    df.plot(kind='line', x=date_name, y='BBU20', ax=ax[0], color='black', label='BBU20', style=['--'])
    df.plot(kind='line', x=date_name, y='BBL20', ax=ax[0], color='black', label='BBL20', style=['--'])
    df.plot(kind='line', x=date_name, y='SMA20', ax=ax[0], color='gray', label='SMA20', style=['--'])
    df.plot(kind='line', x=date_name, y='BBU50', ax=ax[0], color='blue', label='BBU50', style=['.'])
    df.plot(kind='line', x=date_name, y='BBL50', ax=ax[0], color='blue', label='BBL50', style=['.'])
    df.plot(kind='line', x=date_name, y='SMA50', ax=ax[0], color='cyan', label='SMA50', style=['.'])
    df.plot(kind='line', x=date_name, y='Daily', ax=ax[0], color='green', label='Daily')
    df.plot(kind='bar', x=date_name, y='BBTrend', ax=ax[1], label='BBTrend',
            color=plot_color(df['BBTrendType']))
    font = mpl.font_manager.FontProperties(fname='/usr/share/fonts/truetype/arphic-gkai00mp/gkai00mp.ttf')
    ax[0].set_title(title1, fontproperties=font, fontsize='xx-large')
    ax[1].set_title(title2, fontproperties=font, fontsize='xx-large')
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_bbtrend(df, title1, title2, date_name):
    fig = Figure()
    fig.set_figheight(15)
    fig.set_figwidth(15)
    ax = fig.subplots(2)
    df.plot(kind='line', x=date_name, y='BBU20', ax=ax[0], color='black', label='BBU20', style=['--'])
    df.plot(kind='line', x=date_name, y='BBL20', ax=ax[0], color='black', label='BBL20', style=['--'])
    df.plot(kind='line', x=date_name, y='SMA20', ax=ax[0], color='gray', label='SMA20', style=['--'])
    df.plot(kind='line', x=date_name, y='BBU50', ax=ax[0], color='blue', label='BBU50', style=['.'])
    df.plot(kind='line', x=date_name, y='BBL50', ax=ax[0], color='blue', label='BBL50', style=['.'])
    df.plot(kind='line', x=date_name, y='SMA50', ax=ax[0], color='cyan', label='SMA50', style=['.'])
    df.plot(kind='line', x=date_name, y='Daily', ax=ax[0], color='green', label='Daily')
    df.plot(kind='bar', x=date_name, y='BBTrend', ax=ax[1], label='BBTrend',
            color=plot_color(df['BBTrendType']))
    font = mpl.font_manager.FontProperties(fname='/usr/share/fonts/truetype/arphic-gkai00mp/gkai00mp.ttf')
    ax[0].set_title(title1, fontproperties=font, fontsize='xx-large')
    ax[1].set_title(title2, fontproperties=font, fontsize='xx-large')
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_macd(df, title0, title1, title2, date_name):
    fig = Figure()
    fig.set_figheight(20)
    fig.set_figwidth(15)
    ax = fig.subplots(3)
    ax[0].axhline(y=0,  color='black')
    ax[1].axhline(y=0,  color='black')
    df.plot(kind='line', x=date_name, y='Daily', ax=ax[0], color='green', label='Daily', style=['-'])
    df.plot(kind='line', x=date_name, y='macd', ax=ax[0], color='blue', label='MACD', style=['-'])
    df.plot(kind='line', x=date_name, y='macd', ax=ax[1], color='blue', label='MACD', style=['-'])
    df.plot(kind='line', x=date_name, y='signal', ax=ax[1], color='orange', label='signal', style=['-'])
    df.plot(kind='bar', x=date_name, y='Histo', ax=ax[2], label='Histo', color=plot_color(df['MACDType']))
    font = mpl.font_manager.FontProperties(fname='/usr/share/fonts/truetype/arphic-gkai00mp/gkai00mp.ttf')
    ax[0].set_title(title0, fontproperties=font, fontsize='xx-large')
    ax[1].set_title(title1, fontproperties=font, fontsize='xx-large')
    ax[2].set_title(title2, fontproperties=font, fontsize='xx-large')
    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_macd_bb(df, title0, title1, date_name):
    fig = Figure()
    fig.set_figheight(15)
    fig.set_figwidth(15)
    ax = fig.subplots(2)
    ax[0].axhline(y=0,  color='black')

    df.plot(kind='line', x=date_name, y='SMA', ax=ax[0], color='black', label='SMA', style=['--'])
    df.plot(kind='line', x=date_name, y='BBU', ax=ax[0], color='black', label='BBU', style=['-'])
    df.plot(kind='line', x=date_name, y='BBL', ax=ax[0], color='black', label='BBL', style=['-'])
    df.plot(kind='scatter', x=date_name, y='MACD', ax=ax[0], label='MACD', color=plot_macd_bb_color(df['MACDType']))
    df.plot(kind='line', x=date_name, y='MACD', ax=ax[0], label='MACD', color='gray', style=['-'])

    ax1 = ax[1].twinx()
    ax[1].axhline(y=0,  color='black')
    ax[1].set_ylabel('MACD', color='gray')
    ax1.set_ylabel('Daily', color='green')

    df.plot(kind='line', x=date_name, y='MACD', ax=ax[1], label='MACD', color='gray', style=['-'])
    df.plot(kind='scatter', x=date_name, y='MACD', ax=ax[1], color=plot_macd_bb_color(df['MACDType']))
    df.plot(kind='line', x=date_name, y='Daily', ax=ax1, color='green', label='Daily', style=['-'])
    font = mpl.font_manager.FontProperties(fname='/usr/share/fonts/truetype/arphic-gkai00mp/gkai00mp.ttf')
    ax[0].set_title(title0, fontproperties=font, fontsize='xx-large')
    ax[1].set_title(title1, fontproperties=font, fontsize='xx-large')


    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_rsi_bb(df, title0, title1, title2, date_name):
    fig = Figure()
    fig.set_figheight(30)
    fig.set_figwidth(20)
    ax = fig.subplots(3)
    ax[0].axhline(y=30,  color='midnightblue', linestyle='--')
    ax[0].axhline(y=70,  color='midnightblue', linestyle='--')
    ax0 = ax[0].twinx()
    ax[0].set_ylabel('RSI', color='gray')
    ax0.set_ylabel('Daily', color='green')
    df.plot(kind='line', x=date_name, y='RSI_SMA', ax=ax[0], color='black', label='RSI SMA', style=['--'])
    df.plot(kind='line', x=date_name, y='RSI_BBU', ax=ax[0], color='black', label='RSI BBU', style=['-'])
    df.plot(kind='line', x=date_name, y='RSI_BBL', ax=ax[0], color='black', label='RSI BBL', style=['-'])
    df.plot(kind='scatter', x=date_name, y='RSI', ax=ax[0], label='RSI', color=plot_macd_bb_color(df['RSIType']))
    df.plot(kind='line', x=date_name, y='RSI', ax=ax[0], color='gray', style=['-'])
    df.plot(kind='line', x=date_name, y='Daily', ax=ax0, color='green', label='Daily', style=['-'])

    ax[1].axhline(y=30,  color='midnightblue', linestyle='--')
    ax[1].axhline(y=70,  color='midnightblue', linestyle='--')
    ax1 = ax[1].twinx()
    ax[1].set_ylabel('RSI', color='gray')
    ax1.set_ylabel('Daily', color='green')
    df.plot(kind='line', x=date_name, y='RSI', ax=ax[1], label='RSI', color='gray', style=['-'])
    df.plot(kind='line', x=date_name, y='Daily', ax=ax1, color='green', label='Daily', style=['-'])
    df.plot(kind='scatter', x=date_name, y='RSI', ax=ax[1], color=plot_macd_bb_color(df['RSIType']))

    ax2 = ax[2].twinx()
    ax[2].set_ylabel('RSI', color='gray')
    ax2.set_ylabel('Daily', color='green')
    df.plot(kind='line', x=date_name, y='SMA', ax=ax2, color='black', label='SMA', style=['--'])
    df.plot(kind='line', x=date_name, y='BBU', ax=ax2, color='black', label='BBU', style=['-'])
    df.plot(kind='line', x=date_name, y='BBL', ax=ax2, color='black', label='BBL', style=['-'])
    df.plot(kind='line', x=date_name, y='Daily', ax=ax2, color='green', label='Daily', style=['-'])
    df.plot(kind='scatter', x=date_name, y='RSI', ax=ax[2], label='RSI', color=plot_macd_bb_color(df['RSIType']))
    df.plot(kind='line', x=date_name, y='RSI', ax=ax[2], label='RSI', color='gray', style=['-'])

    font = mpl.font_manager.FontProperties(fname='/usr/share/fonts/truetype/arphic-gkai00mp/gkai00mp.ttf')
    ax[0].set_title(title0, fontproperties=font, fontsize='xx-large')
    ax[1].set_title(title1, fontproperties=font, fontsize='xx-large')
    ax[2].set_title(title2, fontproperties=font, fontsize='xx-large')


    # Save it to a temporary buffer.
    buf = BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


def plot_macd_bb_color(values):
    color = []
    for value in values:
        if value == 1:
            color.append('red')
        elif value == 2:
            color.append('orange')
        elif value == 3:
            color.append('aqua')
        elif value == 4:
            color.append('blue')
        else:
            color.append('gray')
    return color