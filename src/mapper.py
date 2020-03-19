
from io_util import read_groups, write_groups
import os

def map(folder_in, folder_out,fn, groups_in,groups_out=0,groupsize=0):
    #read groups from parquet file
    df=read_groups(folder_in,groups_in)

    #apply map function to dataset
    df=fn(df)

    #write mapped data 
    return write_groups(df, "{}/{}.parquet".format(folder_out,os.getpid()), groups_out,groupsize)

