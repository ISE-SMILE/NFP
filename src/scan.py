import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
import math
from pyarrow import fs

class UndefindedGroupsError(Exception):
    pass

def read_groups(path, groups):
    pf=pq.ParquetFile(path)
    #read row groups to pyarrow.Table and convert to pandas DataFrame
    df= pf.read_row_groups(row_groups=groups).to_pandas()

    #cleanup parquet file
    del  pf
    return df

def write_groups(df, path, groups=0, group_size=0):
    #convert DataFrame to pyarrow.Table
    data=pa.Table.from_pandas(df)

    #perserve RAM by deleting the original DataFrame because its not needed anymore
    del df

    #first check if group number is set if not check for group size
    if(groups==0):
        if(group_size!=0):
            groups=data.num_rows/group_size
        
        # if neither groups nor group size is set raise an Error
        else:
            del data
            raise UndefindedGroupsError
   
   #wirte the groups
    pq.write_table(data,path,row_group_size=data.num_rows/groups)
    # cleanup after writing the Table
    del data
    #retrun group count
    return groups

def scan(file_in,file_out,filter_fn,columns,groups_in,groups_out=0,group_size=0):
    #read input groups from file into DataFrame
    df=read_groups(file_in,groups_in)
    # apply all given filter functions
    for fn in filter_fn:
        df=fn(df)
    # select only specific columns
    df=df[columns]

    # write filtered df to parquet file
    write_groups(df,file_out,groups=groups_out,group_size=group_size)

