import os

ClusterJobFilePrefix = "cluster-input-job-"
ClusterJobsScratchDir = os.path.join(os.path.abspath(__file__), "cluster-jobs-scratch-directory")
MolnsExecHelper = os.path.join(os.path.abspath(__file__), "molns_exec_helper.py")
ConfigDir = ".molns"
ClusterExecInputFile = "cluster-exec-input-file"  # Referenced in molns_exec_helper.
