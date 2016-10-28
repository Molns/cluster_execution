import molnsutil.molnsutil.molns_cloudpickle as cloudpickle
import os
import datetime
import constants
from remote_execution import RemoteJob, create_new_id
from cluster_deploy import ClusterDeploy


class ClusterParameterSweep:
    def __init__(self, model_cls, parameters, remote_host, controller_id):
        self.model_cls = model_cls
        self.parameters = parameters
        self.remote_host = remote_host
        self.controller_id = controller_id

    def run(self, mapper, aggregator=None, reducer=None, number_of_trajectories=None, store_realizations=True):

        # Create new remote job.
        job_id = create_new_id()
        input_file_path = os.path.join(constants.ClusterJobsScratchDir, constants.ClusterJobFilePrefix, job_id)
        remote_job = RemoteJob(input_file=input_file_path, date=str(datetime.datetime.now()),
                               remote_host=self.remote_host, remote_job_id=job_id)

        # Write job input file.
        input_data = [self.model_cls, self.parameters, mapper, aggregator, reducer, number_of_trajectories,
                      store_realizations]
        cloudpickle.dump(input_data, input_file_path)

        # Deploy remote job.
        cluster_deploy = ClusterDeploy(remote_job.remote_host)
        cluster_deploy.deploy_cluster_execution_job(remote_job)

        print "Job {0} deployed on the cluster {1}.".format(remote_job, remote_job.remote_host)

        return remote_job
