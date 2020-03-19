import multiprocessing as mp

from scan import scan
from mapper import map
import fnmatch
import io_util as io


def scan_parallel(file_in,file_out,fn,groups_in,groups_out=0,group_size=0):
    pool = mp.Pool(mp.cpu_count())
    result=[pool.apply_async    (scan,args=(file_in,file_out,fn,groups,groups_out,group_size)) for groups in groups_in]
    pool.close()
    pool.join()
    return result

def map_parallel(folder_in, folder_out,fn, groups_in,groups_out=0,groupsize=0):
    pool= mp.Pool(mp.cpu_count())
    files=io.get_parquet_file_names(folder_in)
    result=[pool.apply_async(map,args=("{}/{}".format(folder_in,file_in),folder_out,fn,groups_in,groups_out,groupsize)) for file_in in files]
    pool.close()
    pool.join()
    return result