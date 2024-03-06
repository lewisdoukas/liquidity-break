import datetime, math, time, json, warnings
import pandas as pd
import pandas_ta as ta
import numpy as np
import ccxt
warnings.filterwarnings('ignore')




def find_breaks(df, last_datetime, length, extend, tf_min):
    """ More info about this indicator at https://www.tradingview.com/v/34t0EaMk/"""
    def atan2(df, col):
        angle = 0.0
        y = df[col] - df[f'{col}1']
        x = df['diff']
        if x > 0:
            angle = math.atan(y / x)
        else:
            if x < 0 and y >= 0:
                angle = math.atan(y / x) + math.pi
            else:
                if x < 0 and y < 0:
                    angle = math.atan(y / x) - math.pi
                else:
                    if x == 0 and y > 0:
                        angle = math.pi / 2
                    else:
                        if x == 0 and y < 0:
                            angle = -math.pi / 2
        return(angle)


    def liquidity_break(df):
        vol = df['vol_normal']
        avg = df['sma_vol']
        avg_rank = df['sma_vol_rank']

        if vol < avg:
            return("LV")
        else:
            if vol > avg and vol < avg_rank:
                return("MV")
            else:
                return("HV")


    def check_dt(df, df_break):
        dt = df['dt']
        for index, row in df_break.iterrows():
            if row['break_at'] > dt and index < dt:
                return(True)
        return(False)


    # Calculate volumes and liquity breaks
    l1 = min(75, len(df))
    l2 = min(100, len(df))


    df['vol_wma'] = ta.wma(df['volume'], 21).fillna(0)

    df['vol_normal'] = (df['vol_wma'] - df['vol_wma'].rolling(l2).min()) / (df['vol_wma'].rolling(l2).max() - df['vol_wma'].rolling(l2).min()) * 100
    df['vol_normal'].loc[df['vol_normal'] > 100] = 100
    df['vol_normal'].loc[df['vol_normal'] < 0] = 0

    df['sma_vol'] = df['vol_normal'].expanding().mean()
    df['vol_rank'] = df['vol_normal'].rolling(l1).quantile(1.0, interpolation= "nearest")
    df['sma_vol_rank'] = df['vol_rank'].expanding().mean()
    df['break'] = df.apply(liquidity_break, axis= 1)


    # Calculate pivot high and pivot low
    phd_df = pd.DataFrame()
    pld_df = pd.DataFrame()
    for index, item in df.iterrows():
        if index >= length and index <= len(df) - length - 1:
            tempdf = df.iloc[index - length//2 : index + length//2]

            if df.iloc[index]['high'] >= df.loc[index - length : index + length]['high'].max():
                dmax = df.iloc[index - length//2 : index + length//2]['high'].max()
                if len(tempdf[tempdf['high'] == dmax]) == 1:
                    new_row = {"dt": df.iloc[index]['dt'], "ph": df.iloc[index]['high']}
                    phd_df = pd.concat([phd_df, pd.DataFrame([new_row])], ignore_index=True)

            if df.iloc[index]['low'] <= df.loc[index - length : index + length]['low'].min():
                dmin = df.iloc[index - length//2 : index + length//2]['low'].min()
                if len(tempdf[tempdf['low'] == dmin]) == 1:
                    new_row2 = {"dt": df.iloc[index]['dt'], "pl": df.iloc[index]['low']}
                    pld_df = pd.concat([pld_df, pd.DataFrame([new_row2])], ignore_index=True)


    df.set_index('datetime', inplace= True)


    last_down_break = pd.DataFrame()
    last_up_break = pd.DataFrame()
    final_down_df = pd.DataFrame()
    final_up_df = pd.DataFrame()


    if len(phd_df) > 0:
        phd_df['ph'] = phd_df['ph'].drop_duplicates(keep= "last")
        phd_df.dropna(inplace= True)

        # Calculate last liquidity break for pivot high
        phd_df['dt2'] = phd_df['dt'].copy()
        phd_df.set_index('dt2', inplace= True)
        phd_df['diff'] = ((phd_df['dt'] - phd_df['dt'].shift(1)).dt.total_seconds()/60) // tf_min
        phd_df['diff_last'] = ((last_datetime - phd_df['dt']).dt.total_seconds()/60) // tf_min
        phd_df = phd_df[(phd_df['diff'] >= length) & (phd_df['diff_last'] >= length)]
        phd_df['ph1'] = phd_df['ph'].shift(1)
        phd_df['atan'] = phd_df.apply(atan2, axis= 1, args= ["ph"])
        phd_df['slope'] = (phd_df['ph'] - phd_df['ph1']) / phd_df['diff']

        phd_df = phd_df[phd_df['atan'] <= 0]

        phd_df['break_at'] = np.NAN
        phd_df['break_side'] = np.NAN
        phd_df['volume_y'] = np.NAN


        # Calculate offset
        df['offset'] = (ta.atr(df['high'], df['low'], df['close'], 10) * 6)
        df['offset_pivot'] = df['offset'].shift(-length)


        # Downtrend calcs
        for index, pdf in phd_df.iterrows():
            temp_pdf = pdf.to_frame().T

            new_df_down = df.copy()
            new_df_down = new_df_down.merge(temp_pdf, how= "cross")
            new_df_down['dt_x'] = pd.to_datetime(new_df_down['dt_x'])
            new_df_down['dt_y'] = pd.to_datetime(new_df_down['dt_y'])
            new_df_down['offset_ph'] = new_df_down[new_df_down['dt_x'] == new_df_down['dt_y']]['offset_pivot']
            new_df_down.ffill(inplace= True)

            new_df_down['down_top'] = (new_df_down['ph'] + new_df_down['offset_ph']/7) + (((new_df_down['dt_x']- new_df_down['dt_y']).dt.total_seconds()/60) // tf_min) * new_df_down['slope'] - 7 * new_df_down['slope']
            new_df_down['down_bottom'] = (new_df_down['ph'] - new_df_down['offset_ph'] - new_df_down['offset_ph']/7) + (((new_df_down['dt_x']- new_df_down['dt_y']).dt.total_seconds()/60) // tf_min) * new_df_down['slope'] - 7 * new_df_down['slope']

            new_df_down['side'] = np.where((new_df_down['high'] < new_df_down['down_bottom']), "SELL", np.where((new_df_down['low'] >= new_df_down['down_top']), "BUY", "NONE"))
            first_break = new_df_down[(new_df_down['side'].isin(["BUY", "SELL"])) & (new_df_down['side'].shift(1) == "NONE")]
            if len(first_break) > 0:
                phd_df.loc[index, "break_at"] = first_break['dt_x'].iloc[0]
                phd_df.loc[index, "break_side"] = first_break['side'].iloc[0]
                phd_df.loc[index, "volume_y"] = first_break['volume'].iloc[0]

        # print(phd_df)
        first_nonbreak1 = phd_df[(phd_df['break_side'].isna()) & (phd_df['break_side'].shift(-1).isin(["BUY", "SELL"]))]
        if len(first_nonbreak1) == 0:
            phd_df.dropna(inplace= True)
            try:
                phd_df['test'] = phd_df.apply(check_dt, axis= 1, args= [phd_df])
                phd_df = phd_df[~phd_df['test']]
                final_down_df = phd_df
            except: pass


        if extend and len(final_down_df) > 0:
            final_down_df = phd_df.iloc[-1]
            temp_pdf = final_down_df.to_frame().T

            new_df_down2 = df.copy()
            new_df_down2 = new_df_down2.merge(temp_pdf, how= "cross")
            new_df_down2 = new_df_down2.drop(columns=['break_at', 'break_side', 'volume_y'])
            new_df_down2['dt_x'] = pd.to_datetime(new_df_down2['dt_x'])
            new_df_down2['dt_y'] = pd.to_datetime(new_df_down2['dt_y'])
            new_df_down2['offset_ph'] = new_df_down2[new_df_down2['dt_x'] == new_df_down2['dt_y']]['offset_pivot']
            new_df_down2.ffill(inplace= True)

            new_df_down2['down_top'] = (new_df_down2['ph'] + new_df_down2['offset_ph']/7) + (((new_df_down2['dt_x']- new_df_down2['dt_y']).dt.total_seconds()/60) // tf_min) * new_df_down2['slope'] - 7 * new_df_down2['slope']
            new_df_down2['down_bottom'] = (new_df_down2['ph'] - new_df_down2['offset_ph'] - new_df_down2['offset_ph']/7) + (((new_df_down2['dt_x']- new_df_down2['dt_y']).dt.total_seconds()/60) // tf_min) * new_df_down2['slope'] - 7 * new_df_down2['slope']

            new_df_down2['break_side'] = np.where((new_df_down2['high'] < new_df_down2['down_bottom']), "SELL", np.where((new_df_down2['low'] >= new_df_down2['down_top']), "BUY", "NONE"))
            final_down_df = new_df_down2[(new_df_down2['break_side'].isin(["BUY", "SELL"])) & (new_df_down2['break_side'].shift(1) == "NONE")]
            final_down_df['break_at'] = new_df_down2['dt_x']
            final_down_df['volume_y'] = new_df_down2['volume']


        if len(final_down_df) > 0:
            last_down_break = final_down_df.iloc[-1][['break_at', 'break_side', 'volume_y']]



    if len(pld_df) > 0:  
        pld_df['pl'] = pld_df['pl'].drop_duplicates(keep= "last")
        pld_df.dropna(inplace= True)

        # Calculate last liquidity break for pivot low
        pld_df['dt2'] = pld_df['dt'].copy()
        pld_df.set_index('dt2', inplace= True)
        pld_df['diff'] = ((pld_df['dt'] - pld_df['dt'].shift(1)).dt.total_seconds()/60) // tf_min
        pld_df['diff_last'] = ((last_datetime - pld_df['dt']).dt.total_seconds()/60) // tf_min
        pld_df = pld_df[(pld_df['diff'] >= length) & (pld_df['diff_last'] >= length)]
        pld_df['pl1'] = pld_df['pl'].shift(1)
        pld_df['atan'] = pld_df.apply(atan2, axis= 1, args= ["pl"])
        pld_df['slope'] = (pld_df['pl'] - pld_df['pl1']) / pld_df['diff']

        pld_df = pld_df[pld_df['atan'] >= 0]

        pld_df['break_at'] = np.NAN
        pld_df['break_side'] = np.NAN
        pld_df['volume_y'] = np.NAN


        # Uptrend calcs
        for index, pdf in pld_df.iterrows():
            temp_pdf = pdf.to_frame().T

            new_df_up = df.copy()
            new_df_up = new_df_up.merge(temp_pdf, how= "cross")
            new_df_up['dt_x'] = pd.to_datetime(new_df_up['dt_x'])
            new_df_up['dt_y'] = pd.to_datetime(new_df_up['dt_y'])
            new_df_up['offset_pl'] = new_df_up[new_df_up['dt_x'] == new_df_up['dt_y']]['offset_pivot']
            new_df_up.ffill(inplace= True)

            new_df_up['up_bottom'] = (new_df_up['pl'] - new_df_up['offset_pl']/7) + (((new_df_up['dt_x'] - new_df_up['dt_y']).dt.total_seconds()/60) // tf_min) * new_df_up['slope'] - 7 * new_df_up['slope']
            new_df_up['up_top'] = (new_df_up['pl'] + new_df_up['offset_pl'] + new_df_up['offset_pl']/7) + (((new_df_up['dt_x'] - new_df_up['dt_y']).dt.total_seconds()/60) // tf_min) * new_df_up['slope'] - 7 * new_df_up['slope']

            new_df_up['side'] = np.where((new_df_up['high'] < new_df_up['up_bottom']), "SELL", np.where((new_df_up['low'] >= new_df_up['up_top']), "BUY", "NONE"))
            first_break = new_df_up[(new_df_up['side'].isin(["BUY", "SELL"])) & (new_df_up['side'].shift(1) == "NONE")]
            if len(first_break) > 0:
                pld_df.loc[index, "break_at"] = first_break['dt_x'].iloc[0]
                pld_df.loc[index, "break_side"] = first_break['side'].iloc[0]
                pld_df.loc[index, "volume_y"] = first_break['volume'].iloc[0]
            
        # print(pld_df)
        first_nonbreak2 = pld_df[(pld_df['break_side'].isna()) & (pld_df['break_side'].shift(-1).isin(["BUY", "SELL"]))]
        if len(first_nonbreak2) == 0:
            pld_df.dropna(inplace= True)
            try:
                pld_df['test'] = pld_df.apply(check_dt, axis= 1, args= [pld_df])
                pld_df = pld_df[~pld_df['test']]
                final_up_df = pld_df
            except: pass

        
        if extend and len(final_up_df) > 0:
            final_up_df = pld_df.iloc[-1]
            temp_pdf = final_up_df.to_frame().T

            new_df_up2 = df.copy()
            new_df_up2 = new_df_up2.merge(temp_pdf, how= "cross")
            new_df_up2 = new_df_up2.drop(columns=['break_at', 'break_side', 'volume_y'])
            new_df_up2['dt_x'] = pd.to_datetime(new_df_up2['dt_x'])
            new_df_up2['dt_y'] = pd.to_datetime(new_df_up2['dt_y'])
            new_df_up2['offset_pl'] = new_df_up2[new_df_up2['dt_x'] == new_df_up2['dt_y']]['offset_pivot']
            new_df_up2.ffill(inplace= True)

            new_df_up2['up_bottom'] = (new_df_up2['pl'] - new_df_up2['offset_pl']/7) + (((new_df_up2['dt_x'] - new_df_up2['dt_y']).dt.total_seconds()/60) // tf_min) * new_df_up2['slope'] - 7 * new_df_up2['slope']
            new_df_up2['up_top'] = (new_df_up2['pl'] + new_df_up2['offset_pl'] + new_df_up2['offset_pl']/7) + (((new_df_up2['dt_x'] - new_df_up2['dt_y']).dt.total_seconds()/60) // tf_min) * new_df_up2['slope'] - 7 * new_df_up2['slope']

            new_df_up2['break_side'] = np.where((new_df_up2['high'] < new_df_up2['up_bottom']), "SELL", np.where((new_df_up2['low'] >= new_df_up2['up_top']), "BUY", "NONE"))
            final_up_df = new_df_up2[(new_df_up2['break_side'].isin(["BUY", "SELL"])) & (new_df_up2['break_side'].shift(1) == "NONE")]
            final_up_df['break_at'] = new_df_up2['dt_x']
            final_up_df['volume_y'] = new_df_up2['volume']
        

        if len(final_up_df) > 0:
            last_up_break = final_up_df.iloc[-1][['break_at', 'break_side', 'volume_y']]


    final_candle = last_down_break
    if len(last_up_break) > 0 and len(last_down_break) > 0:
        last_break = str(max(last_up_break['break_at'], last_down_break['break_at']))
        break_candle1 = str(last_up_break['break_at']) == last_break
        final_candle = last_up_break if break_candle1 else last_down_break
    elif len(last_up_break) > 0:
        final_candle = last_up_break

    return(final_candle)






# Initiate bybit connector with ccxt to get OHLCV data
bybit = ccxt.bybit()
symbol = "ETHUSDT"

# Indicator params
length = 8
extend = False
tf = "30m"
tf_min = 30

# Pull data from ccxt and format dataframe
df = pd.DataFrame(bybit.fetch_ohlcv(symbol, timeframe= tf, limit= 200), columns= ["time", "open", "high", "low", "close", "volume"])
df['datetime'] = pd.to_datetime(df['time'], unit="ms")
df['dt'] = df['datetime'].copy()
df = df.iloc[:-1]

last_break = find_breaks(df, length, extend, tf_min)
