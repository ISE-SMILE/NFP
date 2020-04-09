import sys
import os

module_path = os.path.abspath("..") 
if module_path not in sys.path: 
    sys.path.append(module_path)

from nfp.scan import scan_minio

def main(args):
    file_in=args.get("file_in")
    file_out= args.get("file_out")
    fn=args.get("fn")
    groups_in=args.get("groups_in")
    groups_out=args.get("groups_out",0)
    group_size=args.get("group_size", 0)
    print("this is kinda working")
    return {"msg":scan_minio(file_in,file_out,fn,groups_in,groups_out=groups_out)}
    
