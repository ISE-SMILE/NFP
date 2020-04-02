from nfp.common.io_util import read_groups,write_groups, write_groups_minio
import multiprocessing as mp
import os

def scan(file_in,file_out,fn,groups_in,groups_out=0,group_size=0):
    #read input groups from file into DataFrame
    df=read_groups(file_in,groups_in)

    # apply all given filter functions
    df=fn(df)
    
    # write filtered df to parquet file
    write_groups(df,"{}/{}.parquet".format(file_out,os.getpid()),groups=groups_out,group_size=group_size)


def scan_minio(file_in,file_out,fn,groups_in,groups_out=0,group_size=0):
    #read input groups from file into DataFrame
    df=read_groups(file_in,groups_in)

    # apply all given filter functions
    df=fn(df)
    
    # write filtered df to parquet file
    write_groups_minio(df,"{}".format(file_out),groups=groups_out,group_size=group_size)

def scan_parallel(file_in,file_out,fn,groups_in,groups_out=0,group_size=0):
    #create threadpool based on num of cpus
    pool = mp.Pool(mp.cpu_count())
    # start the scan functions
    result=[pool.apply_async    (scan,args=(file_in,file_out,fn,groups,groups_out,group_size)) for groups in groups_in]
    #close the threadpool
    pool.close()
    
    #await termination of the functions
    pool.join()
    return result
