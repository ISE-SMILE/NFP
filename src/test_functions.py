from parallel import scan_parallel, map_parallel
from reducer import reduce
from io_util import read_groups
import pandas
import q6_fn as q6



scan_parallel("../resources/groups.parquet","./test/scanned",q6.filter,[[0,1]],2)
#print(read_groups("./test/test_scan.parquet",[0,1]).shape)


map_parallel("./test/scanned", "./test/mapped",q6.mapper,[0,1],2 )
#print(read_groups("./test/test_map.parquet",[0,1]).shape)


reduce("./test/mapped","./test/test_reduce.parquet",q6.reducer,[0,1],1)
#print(read_groups("./test/test_reduce.parquet",[0]))
