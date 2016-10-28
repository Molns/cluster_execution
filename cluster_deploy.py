import os
import sys
import utils
import constants
from utils import Log
from molns.MolnsLib.ssh_deploy.ssh import SSH


class ClusterDeploy:
    def __init__(self, remote_host):
        self.ssh = SSH()
        self.remote_host = remote_host

    @staticmethod
    def get_files_to_transfer(remote_job):
        import constants
        files_to_transfer = [constants.MolnsExecHelper, remote_job.input_file]
        return files_to_transfer

    def _remotely_install_molnsutil(self, base_path, ssh):
        try:
            sftp = ssh.open_sftp()
            sftp.stat(os.path.join(base_path, 'molnsutil'))
            Log.write_log("molnsutil exists remotely.")
        except (IOError, OSError):
            Log.write_log("Installing molnsutil.")
            ssh.exec_command("""cd {0};
            wget https://github.com/aviral26/molnsutil/archive/qsub_support.zip;
            unzip qsub_support.zip;
            mv molnsutil-qsub_support/molnsutil .;
            rm qsub_support.zip;
            rm -r molnsutil-qsub_support/""".format(base_path))

    def deploy_cluster_execution_job(self, remote_job):
        base_path = os.path.join(constants.MolnsClusterExecutionDir, remote_job.id)
        try:
            self.ssh.connect_cluster_node(ip_address=self.remote_host.ip_address, port=self.remote_host.port,
                                          username=self.remote_host.username,
                                          key_filename=self.remote_host.secret_key_file)
            import pdb
            pdb.set_trace()
            # create remote directory and install packages.
            self.ssh.exec_command("mkdir -p {0}".format(base_path))
            self._remotely_install_molnsutil(constants.MolnsClusterExecutionDir, self.ssh)

            files_to_transfer = ClusterDeploy.get_files_to_transfer(remote_job)

            sftp = self.ssh.open_sftp()

            for f in files_to_transfer:
                utils.Log.write_log('Uploading file {0}'.format(f))
                sftp.put(f, "{0}/{1}".format(base_path, os.path.basename(f)))

            # execute command
            utils.Log.write_log("Executing command..")
            self.ssh.exec_command("cd {0}; python {1} &".format(base_path, os.path.basename(constants.MolnsExecHelper)))
            utils.Log.write_log("Job started.")
        except Exception as e:
            print "Remote execution to start job failed: {0}\n{1}".format(e, remote_job)
            raise sys.exc_info()[1], None, sys.exc_info()[2]
        finally:
            self.ssh.close()

    def remote_execution_job_status(self, remote_job):
        """ Check the status of a remote process.

        Returns: Tuple with two elements: (Is_Running, Message)
            Is_Running: bool    True if the process is running
            Message: str        Description of the status
        """
        base_path = os.path.join(constants.MolnsClusterExecutionDir, remote_job.id)

        try:
            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port
                                          , username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)
            sftp = self.ssh.open_sftp()

            try:
                sftp.stat(os.path.join(base_path, constants.ClusterExecCompleteFile))
            except (IOError, OSError):
                return True, "{0} file not found.".format(constants.ClusterExecCompleteFile)
            finally:
                self.ssh.close()

            try:
                sftp.stat(os.path.join(base_path, constants.ClusterExecOutputFile))
                return False, "{0} file found.".format(constants.ClusterExecOutputFile)
            except (IOError, OSError) as e:
                utils.Log.write_log("{0} does not exist. Job failed.".format(constants.ClusterExecOutputFile))
            finally:
                self.ssh.close()

        except Exception as e:
            print "Remote execution to get job status failed: {0}\n{1}".format(e, remote_job)
            raise sys.exc_info()[1], None, sys.exc_info()[2]

    def remote_execution_get_job_logs(self, remote_job, seek):
        base_path = os.path.join(constants.MolnsClusterExecutionDir, remote_job.id)
        try:
            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port
                                          , username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)
            sftp = self.ssh.open_sftp()
            log = sftp.file(os.path.join(base_path, constants.ClusterExecOutputFile), 'r')
            log.seek(seek)
            output = log.read()
            return output
        except Exception as e:
            print "Remote execution to get job logs failed: {0}\n{1}".format(e, remote_job)
            raise sys.exc_info()[1], None, sys.exc_info()[2]
        finally:
            self.ssh.close()

    def remote_execution_delete_job(self, remote_job):
        base_path = os.path.join(constants.MolnsClusterExecutionDir, remote_job.id)
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
        except Exception as e:
            print "Remote execution to delete job failed: {0}\n{1}".format(e, remote_job)
            raise sys.exc_info()[1], None, sys.exc_info()[2]
        finally:
            self.ssh.close()

    def remote_execution_fetch_file(self, remote_job, remote_file_name, local_file_path):
        base_path = os.path.join(constants.MolnsClusterExecutionDir, remote_job.id)
        try:
            self.ssh.connect_cluster_node(ip_address=remote_job.remote_host.ip_address,
                                          port=remote_job.remote_host.port
                                          , username=remote_job.remote_host.username,
                                          key_filename=remote_job.remote_host.secret_key_file)

            sftp = self.ssh.open_sftp()
            sftp.get(os.path.join(base_path, remote_file_name), local_file_path)
        except Exception as e:
            print "Remote execution to fetch file failed: {0}\n{1}".format(e, remote_job)
            raise sys.exc_info()[1], None, sys.exc_info()[2]
        finally:
            self.ssh.close()
