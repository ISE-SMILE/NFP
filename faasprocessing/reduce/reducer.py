from nfp.common.io_util import read_groups, write_groups, get_parquet_file_names
import pandas

def reduce(folder_in, folder_out,fn, groups_in,groups_out=0,groupsize=0):
    files= get_parquet_file_names(folder_in)
    df=read_groups("{}/{}".format(folder_in,files.pop(0)),groups_in)
    #read groups from parquet files
    for f in files:
        df=df.append(read_groups("{}/{}".format(folder_in,f),groups_in))
    
    #apply reduce function to dataset
    df=fn(df)

    #write mapped data 
    return write_groups(df, folder_out, groups_out,groupsize)