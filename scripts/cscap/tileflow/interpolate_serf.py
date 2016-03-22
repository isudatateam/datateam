import pandas as pd

df = pd.read_excel("/tmp/SERF Drainage Data.xlsx", sheetname=None)

for plotid in df:
    if plotid != 'Plot3':
        continue
    df[plotid]['valid'] = df[plotid][['Year', 'Month', 'Day', 'Time']].apply(
        lambda x: "%.0f/%02.0f/%02.0f %s" % (x[0], x[1], x[2], x[3]), axis=1)
    df[plotid]['valid'] = pd.to_datetime(df[plotid]['valid'],
                                         format='%Y/%m/%d %H:%M:%S',
                                         errors='coerce')
    df[plotid].set_index('valid', inplace=True)
    df[plotid] = df[plotid][pd.notnull(df[plotid].index)]
    print plotid, len(df[plotid].index)
    tdf = df[plotid].copy()
    tdf = tdf[tdf['Nitrate concentration, mg/L'].notnull()].copy()

    #tidx = 0
    #tvalid = tdf.iloc(0)['valid']
    #for i, row in df[plotid].iterrows():
    #    
    df[plotid]['Interpolated Nitrate Concentration, mg/L'] = (
        df[plotid]['Nitrate concentration, mg/L'].interpolate(method='time'))


writer = pd.ExcelWriter('output.xlsx')
for plotid in df:
    df[plotid].to_excel(writer, plotid)
writer.save()
