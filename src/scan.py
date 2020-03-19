from io_util import read_groups,write_groups
import os

#TODO change signiture of function to folder as input
def scan(file_in,file_out,fn,groups_in,groups_out=0,group_size=0):
    #read input groups from file into DataFrame
    df=read_groups(file_in,groups_in)

    # apply all given filter functions
    df=fn(df)
    
    # write filtered df to parquet file
    write_groups(df,"{}/{}.parquet".format(file_out,os.getpid()),groups=groups_out,group_size=group_size)
    