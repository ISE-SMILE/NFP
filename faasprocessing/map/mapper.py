
from faasprocessing.common.io_util import read_groups, write_groups, get_parquet_file_names, read_groups_minio, write_groups_minio
import os
import multiprocessing as mp


def map(folder_in, folder_out,fn, groups_in,groups_out=0,groupsize=0):
    #read groups from parquet file
    df=read_groups(folder_in,groups_in)

    #apply map function to dataset
    df=fn(df)

    #write mapped data 
    return write_groups(df, "{}/{}.parquet".format(folder_out,os.getpid()), groups_out,groupsize)

def map_minio(folder_in, folder_out,fn, groups_in,groups_out=0,groupsize=0):
    #read input groups from file into DataFrame
    df=read_groups_minio(file_in,groups_in)

    # apply all given mapper functions
    df=fn(df)
    
    # write filtered df to parquet file
    return write_groups_minio(df,"{}".format(file_out),groups=groups_out,group_size=group_size)

def map_parallel(folder_in, folder_out,fn, groups_in,groups_out=0,groupsize=0):
    # create threadpool based on # of cpus
    pool= mp.Pool(mp.cpu_count())

    #list all parquetfiles in the given folder
    files=get_parquet_file_names(folder_in)

    #execute functions 
    result=[pool.apply_async(map,args=("{}/{}".format(folder_in,file_in),folder_out,fn,groups_in,groups_out,groupsize)) for file_in in files]
    
    #close threadpool
    pool.close()
    
    #await function termination
    pool.join()
    return result