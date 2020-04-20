import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
import math
import os
import fnmatch
import s3fs



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
   
    #write the groups
    pq.write_table(data,path,row_group_size=data.num_rows/groups)
    # cleanup after writing the Table
    del data
    #retrun group count
    return groups

def get_parquet_file_names(folder):
    files=[]
    for file in os.listdir(folder):
        if fnmatch.fnmatch(file, '*.parquet'):
            files.append(file)
    return files


def write_groups_minio(df, path, groups=0, group_size=0):
    #convert DataFrame to pyarrow.Table
    data=pa.Table.from_pandas(df)

    #perserve RAM by deleting the original DataFrame because its not needed anymore
    del df

    
    minio_access_key = 'minioadmin'
    minio_secret_key = 'minioadmin'
    endpoint = '172.17.0.11:9000'
    client_kwargs = {'endpoint_url': 'http://' + endpoint}
    fs = s3fs.S3FileSystem(key=minio_access_key, secret=minio_secret_key,client_kwargs=client_kwargs)


    bucket_uri = 's3://{0}'.format(path)
    fs.mkdir(bucket_uri)
    #first check if group number is set if not check for group size
    if(groups==0):
        if(group_size!=0):
            groups=math.ceil(data.num_rows/group_size)
        
        # if neither groups nor group size is set raise an Error
        else:
            del data
            raise UndefindedGroupsError

    #write the groups
    pq.write_table(data,"{}/{}.parquet".format(path,os.getpid()),row_group_size=data.num_rows/groups, filesystem=fs)
    # cleanup after writing the Table
    del data
    #return group count
    return groups

def read_groups_minio(path, groups):
    # set minio credentials 
    minio_access_key = 'minioadmin'
    minio_secret_key = 'minioadmin'
    endpoint = '172.17.0.11:9000'
    client_kwargs = {'endpoint_url': 'http://' + endpoint}
    fs = s3fs.S3FileSystem(key=minio_access_key, secret=minio_secret_key,client_kwargs=client_kwargs)

    #create the directory in the bucket
    bucket_uri = 's3://{0}'.format(path)
    fs.mkdir(bucket_uri)
    
    #define custom opne function
    myopen=fs.open

    #hacky way to read a ParquetFile from s3
    pf=pq.ParquetDatasetPiece(bucket_uri,myopen).open()

    #read the given row groups and conver tthem to pandas
    df= pf.read_row_groups(row_groups=groups).to_pandas()
    
    #cleanup parquet file
    del  pf
    return df



