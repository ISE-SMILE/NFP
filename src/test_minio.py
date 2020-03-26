import q6_fn as q6
from scan import scan_minio


scan_minio("../resources/groups.parquet","test/scanned",q6.filter,[0,1],2)
