import os
from cluster_parameter_sweep import ClusterParameterSweep
from remote_execution import RemoteHost
from cluster_execution_exceptions import IncorrectRemoteHostSpec, ClusterExecutionException


remote_host_address = None
remote_host_username = None
remote_host_secret_key_file = None
remote_host_ssh_port = 22


def __verify_remote_parameters():
    if remote_host_address is None:
        raise IncorrectRemoteHostSpec("remote_host_address not set.")

    if remote_host_username is None:
        raise IncorrectRemoteHostSpec("remote_host_username not set.")

    if remote_host_secret_key_file is None or not os.access(remote_host_secret_key_file, os.R_OK):
        raise IncorrectRemoteHostSpec("Please verify remote_host_secret_key_file.")


def get_remote_host():
    __verify_remote_parameters()
    return RemoteHost(ip_address=remote_host_address, username=remote_host_username,
                      secret_key_file=remote_host_secret_key_file, port=remote_host_ssh_port)


class ParameterSweep(ClusterParameterSweep):
    def __init__(self, model_class=None, parameters=None):
        ClusterParameterSweep.__init__(self, model_cls=model_class, parameters=parameters,
                                       remote_host=get_remote_host())

    def run(self, mapper=None, reducer=None, aggregator=None, store_realizations=False, number_of_trajectories=None):
        remote_job = self.run_async(mapper=mapper, reducer=reducer, aggregator=aggregator,
                                    store_realizations=store_realizations,
                                    number_of_trajectories=number_of_trajectories)
        print "Waiting for results to be computed..."
        return self.get_results(remote_job)


class DistributedEnsemble(ClusterParameterSweep):
    def __init__(self, model_class):
        ClusterParameterSweep.__init__(self, model_cls=model_class, parameters=None, remote_host=get_remote_host())

    def add_realizations(self, number_of_trajectories=None):
        if number_of_trajectories is None:
            raise ClusterExecutionException("Number of trajectories cannot be None.")

        remote_job = self.run_async(number_of_trajectories=number_of_trajectories, add_realizations=True)
        print "Generating {0} realizations...".format(number_of_trajectories)
        return self.get_results(remote_job)
