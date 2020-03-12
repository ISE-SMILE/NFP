from scan import scan
import mapper as mp
from reducer import reduce
from io_util import read_groups
import pandas


def q6_filter(df: pandas.DataFrame) -> pandas.DataFrame:
    after_1994=df['L_SHIPDATE']>='1994-01-01'
    before_1995=df['L_SHIPDATE']<'1995-01-01'
    greater_d=df['L_DISCOUNT']>0.05
    lower_d=df['L_DISCOUNT']<0.07
    lower_q=df['L_QUANTITY']<24
    return df[greater_d & lower_d &lower_q & before_1995 &after_1994]

scan("../resources/groups.parquet","./test/test_scan.parquet",q6_filter,['L_EXTENDEDPRICE', 'L_DISCOUNT'],[0,1],2)
print(read_groups("./test/test_scan.parquet",[0,1]).shape)

def q6_mapper(df):
    out=pandas.DataFrame(columns=['group','value'])
    for index,row in df.iterrows():
        out=out.append({'group':1,'value':row['L_DISCOUNT']*row['L_EXTENDEDPRICE']}, ignore_index=True)
    return out


mp.map("./test/test_scan.parquet", "./test/test_map.parquet",q6_mapper,[0,1],2 )
print(read_groups("./test/test_map.parquet",[0,1]).shape)

def q6_reducer(df):
    sum=0
    for index,row in df.iterrows():
        sum+=row['value']
    return pandas.DataFrame([['result',sum]],columns=['result', 'value'])

reduce("./test/test_map.parquet","./test/test_reduce.parquet",q6_reducer,[0,1],1)
print(read_groups("./test/test_reduce.parquet",[0]))
