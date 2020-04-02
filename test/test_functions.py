import sys
import os

module_path = os.path.abspath("..") 
if module_path not in sys.path: 
    sys.path.append(module_path)


from nfp.scan import scan_parallel
from nfp.reduce import reduce
from nfp.common import read_groups
from nfp.map import map_parallel
import pandas
import q6_fn as q6


scan_parallel("./resources/groups.parquet","./resources/scanned",q6.filter,[[0,1]],2)
#print(read_groups("./test/test_scan.parquet",[0,1]).shape)


map_parallel("./resources/scanned", "./resources/mapped",q6.mapper,[0,1],2 )
#print(read_groups("./test/test_map.parquet",[0,1]).shape)


reduce("./resources/mapped","./resources/test_reduce.parquet",q6.reducer,[0,1],1)
#print(read_groups("./test/test_reduce.parquet",[0]))
