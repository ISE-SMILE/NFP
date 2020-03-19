import pandas

def filter(df: pandas.DataFrame) -> pandas.DataFrame:
    after_1994=df['L_SHIPDATE']>='1994-01-01'
    before_1995=df['L_SHIPDATE']<'1995-01-01'
    greater_d=df['L_DISCOUNT']>0.05
    lower_d=df['L_DISCOUNT']<0.07   
    lower_q=df['L_QUANTITY']<24
    df=df[greater_d & lower_d &lower_q & before_1995 &after_1994]
    df=df[['L_EXTENDEDPRICE', 'L_DISCOUNT']]
    return df

def mapper(df):
    out=pandas.DataFrame(columns=['group','value'])
    for index,row in df.iterrows():
        out=out.append({'group':1,'value':row['L_DISCOUNT']*row['L_EXTENDEDPRICE']}, ignore_index=True)
    return out

def reducer(df):
    sum=0
    for index,row in df.iterrows():
        sum+=row['value']
    return pandas.DataFrame([['result',sum]],columns=['result', 'value'])
