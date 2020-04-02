from nfp.parallel import  scan_parallel, map_parallel
from nfp.reducer import reduce
import q6_fn as q6
import os
import shutil


scanned="./test/scanned"
mapped="./test/mapped"
shutil.rmtree(scanned)
shutil.rmtree(mapped)
os.mkdir(scanned)
os.mkdir(mapped)


scan_parallel("../resources/groups.parquet",scanned,q6.filter,[[0],[1]],2)
#print(read_groups("./test/test_scan.parquet",[0,1]).shape)
#print("scanning completed")     
map_parallel(scanned, mapped,q6.mapper,[0,1],2 )

reduce(mapped,"./test/test_reduce_p.parquet",q6.reducer,[0,1],1)