import os

ClusterJobFilePrefix = "cluster-input-job-"
PickledClusterInputFile = "pickled-cluster-input-file"
ClusterJobsScratchDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cluster-jobs-scratch-directory")
MolnsExecHelper = os.path.join(os.path.dirname(os.path.abspath(__file__)), "molns_exec_helper.py")
ConfigDir = ".molns"
ClusterExecInputFile = "cluster-exec-input-file"  # Referenced in molns_exec_helper.
ClusterExecOutputFile = "cluster-exec-output-file"  # Referenced in molns_exec_helper.
ClusterExecInfoLogsFile = "molns_exec_helper.log.info"
ClusterExecDebugLogsFile = "molns_exec_helper.log.debug"
ClusterExecSuperLogsFile = "molns_exec_helper.log"
ClusterExecCompleteFile = "cluster-exec-job-complete"
MolnsClusterExecutionDir = ".molns_cluster_exec"
DefaultSshPort = 22
RemoteJobRunning = 0
RemoteJobCompleted = 1
RemoteJobFailed = 2
