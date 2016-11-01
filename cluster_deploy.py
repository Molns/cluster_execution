import os
import utils
import constants
from utils import Log
from molns.MolnsLib.ssh_deploy.ssh import SSH, SSHException


class ClusterDeploy:
    def __init__(self, remote_host):
        self.ssh = SSH()
        self.remote_host = remote_host

    @staticmethod
    def __get_files_to_transfer(remote_job):
        import constants
        files_to_transfer = [constants.MolnsExecHelper, remote_job.input_file]
        Log.write_log("Files to transfer: {0}".format(files_to_transfer))
        return files_to_transfer

    @staticmethod
    def __remotely_install_molnsutil(base_path, ssh):
        try:
            sftp = ssh.open_sftp()
            sftp.stat(os.path.join(base_path, 'molnsutil'))
            Log.write_log("molnsutil exists remotely.")
        except (IOError, OSError):
            Log.write_log("Installing molnsutil..")
            Log.write_log("Installing in {0}".format(base_path))
            ssh.exec_command("""cd {0};
            wget https://github.com/aviral26/molnsutil/archive/qsub_support.zip;
            unzip qsub_support.zip;
            mv molnsutil-qsub_support/ molnsutil/;
            rm qsub_support.zip;""".format(base_path))

    def deploy_job_to_cluster(self, remote_job):
        try:
            base_path = os.path.join(constants.MolnsClusterExecutionDir, remote_job.id)
            self.ssh.connect_cluster_node(ip_address=self.remote_host.ip_address, port=self.remote_host.port,
                                          username=self.remote_host.username,
                                          key_filename=self.remote_host.secret_key_file)

            # create remote directory and install packages.
            self.ssh.exec_command("mkdir -p {0}".format(base_path))
            self.__remotely_install_molnsutil(constants.MolnsClusterExecutionDir, self.ssh)

            files_to_transfer = ClusterDeploy.__get_files_to_transfer(remote_job)

            sftp = self.ssh.open_sftp()

            for f in files_to_transfer:
                utils.Log.write_log('Uploading file {0}'.format(f))
                sftp.put(f, "{0}/{1}".format(base_path, os.path.basename(f)))

            # execute command
            utils.Log.write_log("Executing command..")
            self.ssh.exec_command("cd {0}; python {1} &".format(base_path, os.path.basename(constants.MolnsExecHelper)))
            utils.Log.write_log("Job started.")
        finally:
            self.ssh.close()

    def job_status(self, remote_job):
        base_path = os.path.join(constants.MolnsClusterExecutionDir, remote_job.id)
        try:
            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port
                                          , username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)
            sftp = self.ssh.open_sftp()
            import pdb
            pdb.set_trace()
            try:
                sftp.stat(os.path.join(base_path, constants.ClusterExecCompleteFile))
            except (IOError, OSError):
                return constants.RemoteJobRunning

            try:
                sftp.stat(os.path.join(base_path, constants.ClusterExecOutputFile))
                return constants.RemoteJobCompleted
            except (IOError, OSError):
                return constants.RemoteJobFailed
        finally:
            self.ssh.close()

    def get_job_logs(self, remote_job, seek=0):
        try:
            base_path = os.path.join(constants.MolnsClusterExecutionDir, remote_job.id)
            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port, username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)
            sftp = self.ssh.open_sftp()
            log = sftp.file(os.path.join(base_path, constants.ClusterExecOutputFile), 'r')
            log.seek(seek)
            output = log.read()
            return output
        finally:
            self.ssh.close()

    def clean_up(self, remote_job):
        try:
            base_path = os.path.join(constants.MolnsClusterExecutionDir, remote_job.id)

            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port, username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)

            # If process is still running, terminate it.
            try:
                self.ssh.exec_command("kill -TERM `cat {0}/pid` > /dev/null 2&>1".format(base_path))
            except SSHException:
                Log.write_log("Remote process already not running.")

            # Remove the job directory on the remote server.
            self.ssh.exec_command("rm -r {0}".format(base_path))

            # Clear out scratch directory entries.
            os.rmdir(remote_job.local_scratch_dir)
        finally:
            self.ssh.close()

    def fetch_remote_job_file(self, remote_job, remote_file_name, local_file_path):
        try:
            base_path = os.path.join(constants.MolnsClusterExecutionDir, remote_job.id)
            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port, username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)

            sftp = self.ssh.open_sftp()
            sftp.get(os.path.join(base_path, remote_file_name), os.path.join(local_file_path,
                                                                             constants.ClusterExecOutputFile))
        finally:
            self.ssh.close()
