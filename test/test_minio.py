import q6_fn as q6
from faasprocessing.scan import scan_minio
from faasprocessing.common.io_util import read_groups_minio

scan_minio("./resources/groups.parquet","test/scanned",q6.filter,[0,1],2)
out=read_groups_minio("test/scanned/17511.parquet",[0,1])
del out