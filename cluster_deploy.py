import os
import sys
import utils
import constants
from molns.MolnsLib.ssh_deploy.ssh_deploy import SSHDeployException
from molns.MolnsLib.ssh_deploy.ssh import SSH


# TODO make this class compatible with molns_exec_helper
class ClusterDeploy:
    def __init__(self, remote_host):
        self.ssh = SSH()
        self.remote_host = remote_host

    def _remotely_install_molnsutil(self, base_path):
        self.ssh.exec_command("cd {0}".format(base_path))
        self.ssh.exec_command("wget https://github.com/aviral26/molnsutil/archive/qsub_support.zip")
        self.ssh.exec_command("unzip qsub_support.zip")
        self.ssh.exec_command("mv molnsutil-qsub_support/molnsutil .")
        self.ssh.exec_command("rm qsub_support.zip")
        self.ssh.exec_command("rm -r molnsutil-qsub_support/")

    def deploy_cluster_execution_job(self, remote_job):
        base_path = "{0}".format(remote_job.id)

        try:
            self.ssh.connect_cluster_node(ip_address=self.remote_host.ip_address, port=self.remote_host.port,
                                          username=self.remote_host.username,
                                          key_filename=self.remote_host.secret_key_file)

            # create remote directory
            self.ssh.exec_command("mkdir -p {0}".format(base_path))
            self.ssh.exec_command("mkdir -p {0}/.molns/".format(base_path))
            self._remotely_install_molnsutil(base_path)

            files_to_transfer = utils.get_files_to_transfer(remote_job)

            sftp = self.ssh.open_sftp()

            for f in files_to_transfer:
                utils.Log.write_log('Uploading file {0}'.format(f))
                sftp.put(f, "{0}/{1}".format(base_path, os.path.basename(f)))

            # execute command
            utils.Log.write_log("Executing command..")
            self.ssh.exec_command("cd {0}; python .molns/{1} &".format(base_path, constants.MolnsExecHelper))
            self.ssh.close()
            utils.Log.write_log("Job started.")
        except Exception as e:
            print "Remote execution to start job failed: {0}\n{1}".format(e, remote_job)
            raise sys.exc_info()[1], None, sys.exc_info()[2]

    def remote_execution_job_status(self, remote_job):
        """ Check the status of a remote process.

        Returns: Tuple with two elements: (Is_Running, Message)
            Is_Running: bool    True if the process is running
            Message: str        Description of the status
        """
        base_path = "{0}".format(remote_job.id)

        try:
            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port
                                          , username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)
            sftp = self.ssh.open_sftp()

            # Does the 'pid' file exists remotely?
            try:
                sftp.stat("{0}/pid".format(base_path))
            except (IOError, OSError) as e:
                self.ssh.close()
                raise SSHDeployException("Remote process not started. (Pid file not found.)")

            # Does the 'return_value' file exist?
            try:
                sftp.stat("{0}/return_value".format(base_path))
                # Process is complete
                return False, "Remote process finished"
            except (IOError, OSError) as e:
                utils.Log.write_log("'return_value' does not exist yet. Exception caught: {0]".format(str(e)))

            # is the process running?
            try:
                self.ssh.exec_command("kill -0 `cat {0}/pid` > /dev/null 2&>1".format(base_path))
                return True, "Remote process running"
            except SSHDeployException as e:
                raise SSHDeployException(
                    "Remote process not running. (Process not found.) Exception: {0}".format(e))
            finally:
                self.ssh.close()
        except Exception as e:
            print "Remote execution to get hob status failed: {0}\n{1}".format(e, remote_job)
            raise sys.exc_info()[1], None, sys.exc_info()[2]

    def remote_execution_get_job_logs(self, remote_job, seek):
        base_path = "{0}".format(remote_job.id)
        try:
            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port
                                          , username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)
            sftp = self.ssh.open_sftp()
            log = sftp.file("{0}/molns-exec-helper-stdout".format(base_path), 'r')
            log.seek(seek)
            output = log.read()
            self.ssh.close()
            return output
        except Exception as e:
            print "Remote execution to get job logs failed: {0}\n{1}".format(e, remote_job)
            raise sys.exc_info()[1], None, sys.exc_info()[2]

    def remote_execution_delete_job(self, remote_job):
        base_path = "{0}".format(remote_job.id)
        try:
            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port
                                          , username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)

            # If process is still running, terminate it.
            try:
                self.ssh.exec_command("kill -TERM `cat {0}/pid` > /dev/null 2&>1".format(base_path))
            except:
                pass
            # Remove the job directory on the remote server.
            self.ssh.exec_command("rm -r {0}".format(base_path))
            self.ssh.close()
        except Exception as e:
            print "Remote execution to delete job failed: {0}\n{1}".format(e, remote_job)
            raise sys.exc_info()[1], None, sys.exc_info()[2]

    def remote_execution_fetch_file(self, remote_job, remote_file, local_file_name):
        base_path = "{0}".format(remote_job.id)
        try:
            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port
                                          , username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)

            sftp = self.ssh.open_sftp()
            sftp.get("{0}/{1}".format(base_path, remote_file), local_file_name)
            self.ssh.close()
        except Exception as e:
            print "Remote execution to fetch file failed: {0}\n{1}".format(e, remote_job)
            raise sys.exc_info()[1], None, sys.exc_info()[2]
