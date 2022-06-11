import pandas as pd

def ClearDf(df,columns):
    dff = df
    for c in columns:
        dff[c] = [float(str(x).replace(',','')) for x in dff[c]]
    return dff