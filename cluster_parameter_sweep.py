import molnsutil.molns_cloudpickle as cloudpickle
import os
import datetime
import constants
import cluster_execution_exceptions
from remote_execution import RemoteJob, create_new_id
from cluster_deploy import ClusterDeploy
from utils import Log


class ClusterParameterSweep:
    def __init__(self, model_cls, parameters, remote_host):
        self.model_cls = model_cls
        self.parameters = parameters
        self.remote_host = remote_host
        self.cluster_deploy = ClusterDeploy(remote_host)

    def run_async(self, mapper, aggregator=None, reducer=None, number_of_trajectories=None, store_realizations=True):
        # Create new remote job.
        job_id = create_new_id()

        input_file_dir = os.path.join(constants.ClusterJobsScratchDir, constants.ClusterJobFilePrefix + job_id)
        if not os.path.exists(input_file_dir):
            os.makedirs(input_file_dir)

        # module_files = self.__get_module_files_required(mapper, aggregator, reducer)

        # Write job input file.
        input_data = {'model_cls': self.model_cls, 'params': self.parameters, 'mapper': mapper, 'aggregator': aggregator
                      , 'reducer': reducer, 'number_of_trajectories': number_of_trajectories,
                      'store_realizations': store_realizations}

        input_file_path = os.path.join(input_file_dir, constants.ClusterExecInputFile)
        with open(input_file_path, "wb") as input_file:
            cloudpickle.dump(input_data, input_file)

        remote_job = RemoteJob(input_file=input_file_path, date=str(datetime.datetime.now()),
                               remote_host=self.remote_host, remote_job_id=job_id, local_scratch_dir=input_file_dir)

        # Deploy remote job.
        self.cluster_deploy.deploy_job_to_cluster(remote_job)

        Log.write_log("Deployed\n{0}".format(str(remote_job)))

        return remote_job

    # TODO make this an async method
    def get_sweep_result(self, remote_job):
        job_status = self.cluster_deploy.job_status(remote_job)

        if job_status == constants.RemoteJobRunning:
            raise cluster_execution_exceptions.RemoteJobNotFinished("The parameter sweep has not finished yet.")

        if job_status == constants.RemoteJobFailed:
            job_logs = self.cluster_deploy.get_job_logs(remote_job)
            raise cluster_execution_exceptions.RemoteJobFailed("Failed to do parameter sweep. Job logs:\n{0}"
                                                               .format(job_logs))

        assert job_status == constants.RemoteJobCompleted

        if remote_job.local_scratch_dir is None:
            raise cluster_execution_exceptions.UnknownScratchDir("Scratch directory location unknown for {0}"
                                                                 .format(remote_job))
        self.cluster_deploy.fetch_remote_job_file(remote_job, constants.ClusterExecOutputFile,
                                                  remote_job.local_scratch_dir)

        return cloudpickle.load(os.path.join(remote_job.local_scratch_dir, constants.ClusterExecOutputFile))

    def clean_up(self, remote_job):
        self.cluster_deploy.clean_up(remote_job)

    def __get_module_files_required(self, mapper, aggregator, reducer):
        import sys
        import inspect

        modules_files = set()

        check_objs = [self.model_cls, mapper, aggregator, reducer]

        for check_obj in check_objs:
            if check_obj is None:
                continue
            m = check_obj.__module__
            module_file = os.path.abspath(inspect.getsourcefile(sys.modules[m]))
            if not os.access(module_file, os.R_OK):
                raise cluster_execution_exceptions.ModuleFileNotReadable("Cannot read module file {0}"
                                                                         .format(module_file))
            modules_files.add(module_file)

        return modules_files
