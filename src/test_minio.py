import q6_fn as q6
from scan import scan_minio
from io_util import read_groups_minio

scan_minio("../resources/groups.parquet","test/scanned",q6.filter,[0,1],2)
out=read_groups_minio("test/scanned/3189.parquet",[0,1])
del out