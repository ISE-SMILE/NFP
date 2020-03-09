from scan import scan,read_groups
import pandas

def ship_date_filter(df):
    after_1994=df['L_SHIPDATE']>='1994-01-01'
    before_1995=df['L_SHIPDATE']<'1995-01-01'
    return df[after_1994&before_1995]

def discount_in_range_filter(df: pandas.DataFrame) -> pandas.DataFrame:
    print(df.dtypes)
    greater=df['L_DISCOUNT']>0.05
    lower=df['L_DISCOUNT']<0.07
    return df[greater & lower]

def quantity_filter(df):
    lower=df['L_QUANTITY']<24
    return df[lower]

scan("../groups/groups.parquet","./test/test_scan.parquet",[ship_date_filter,discount_in_range_filter, quantity_filter],['L_EXTENDEDPRICE', 'L_DISCOUNT'],[0,1],2)
print(read_groups("./test/test_scan.parquet",[0,1]).shape)