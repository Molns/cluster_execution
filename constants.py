import os

ClusterJobFilePrefix = "cluster-input-job-"
ClusterJobsScratchDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cluster-jobs-scratch-directory")
MolnsExecHelper = os.path.join(os.path.dirname(os.path.abspath(__file__)), "molns_exec_helper.py")
ConfigDir = ".molns"
ClusterExecInputFile = "cluster-exec-input-file"  # Referenced in molns_exec_helper.
ClusterExecOutputFile = "cluster-exec-output-file"  # Referenced in molns_exec_helper.
ClusterExecLogsFile = "molns_exec_helper_logs"
ClusterExecCompleteFile = "cluster-exec-job-complete"
MolnsClusterExecutionDir = ".molns_cluster_exec"
DefaultSshPort = 22
RemoteJobRunning = 0
RemoteJobCompleted = 1
RemoteJobFailed = 2
