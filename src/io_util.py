import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
import math


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
            groups=math.ceil(data.num_rows/group_size)
        
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