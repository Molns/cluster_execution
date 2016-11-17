import os
from cluster_parameter_sweep import ClusterParameterSweep
from remote_execution import RemoteHost
from cluster_execution_exceptions import IncorrectRemoteHostSpec, ClusterExecutionException
from molns.MolnsLib.constants import Constants
from molnsutil.utils import builtin_aggregator_sum_and_sum2, builtin_reducer_mean_variance, builtin_aggregator_add, \
    builtin_reducer_mean


remote_host_address = None
remote_host_username = None
remote_host_secret_key_file = os.path.join("/home/ubuntu", Constants.ClusterKeyFileNameOnController)
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


class DistributedEnsemble(ClusterParameterSweep):
    def __init__(self, model_class, parameters=None):
        ClusterParameterSweep.__init__(self, model_cls=model_class, parameters=parameters, remote_host=get_remote_host())
        self.realizations_storage_dir = None
        self.remote_job = None

    def add_realizations(self, number_of_trajectories=None):
        if number_of_trajectories is None:
            raise ClusterExecutionException("Number of trajectories cannot be None.")

        self.remote_job = self.run_async(number_of_trajectories=number_of_trajectories, add_realizations=True)
        print "Generating {0} realizations...".format(number_of_trajectories)
        import json
        res = json.loads(self.get_results(self.remote_job, add_realizations=True))
        self.realizations_storage_dir = res["realizations_directory"]
        return res

    def run(self, mapper=None, reducer=None, aggregator=None, store_realizations=False, number_of_trajectories=None):
        mapper = DistributedEnsemble.__get_mapper(mapper)
        self.remote_job = self.run_async(mapper=mapper, reducer=reducer, aggregator=aggregator,
                                         store_realizations=store_realizations,
                                         number_of_trajectories=number_of_trajectories)
        print "Waiting for results to be computed..."
        return self.get_results(self.remote_job)

    def mean_variance(self, mapper=None, realizations_storage_directory=None):
        """ Compute the mean and variance (second order central moment) of the function g(X) based on
        number_of_trajectories realizations in the ensemble. """

        mapper = DistributedEnsemble.__get_mapper(mapper)

        realizations_storage_directory = self.__get_realizations_storage_directory(realizations_storage_directory)

        self.remote_job = self.run_async(mapper=mapper, aggregator=builtin_aggregator_sum_and_sum2,
                                         reducer=builtin_reducer_mean_variance,
                                         realizations_storage_directory=realizations_storage_directory)
        print "Waiting for results to be computed..."
        return self.get_results(self.remote_job)

    def mean(self, mapper=None, realizations_storage_directory=None):
        """ Compute the mean of the function g(X) based on number_of_trajectories realizations
            in the ensemble. It has to make sense to say g(result1)+g(result2). """

        mapper = DistributedEnsemble.__get_mapper(mapper)

        realizations_storage_directory = self.__get_realizations_storage_directory(realizations_storage_directory)

        self.remote_job = self.run_async(mapper=mapper, aggregator=builtin_aggregator_add, reducer=builtin_reducer_mean,
                                         realizations_storage_directory=realizations_storage_directory)
        print "Waiting for results to be computed..."
        return self.get_results(self.remote_job)

    def clean_up(self):
        ClusterParameterSweep.clean_up(self, remote_job=self.remote_job)

    def __get_realizations_storage_directory(self, directory):
        if directory is not None:
            return directory
        if self.realizations_storage_dir is None:
            raise ClusterExecutionException("Something went wrong. Unknown realizations storage directory.")
        return self.realizations_storage_dir

    @staticmethod
    def __get_mapper(mapper):
        if mapper is not None:
            return mapper
        else:
            raise ClusterExecutionException("No default mapper available. Please specify one.")


class ParameterSweep(DistributedEnsemble):
    def __init__(self, model_class=None, parameters=None):
        DistributedEnsemble.__init__(self, model_class=model_class, parameters=parameters)
