import os
import constants


def create_new_id():
    import uuid
    return str(uuid.uuid4())


class RemoteHostException(Exception):
    pass


class RemoteHost:
    def __init__(self, ip_address,  username, secret_key_file, port=constants.DefaultSshPort,
                 remote_host_id=None):
        if not os.path.exists(secret_key_file):
            raise RemoteHostException("Cannot access {0}".format(secret_key_file))

        self.ip_address = ip_address
        self.port = port
        self.username = username
        self.secret_key_file = secret_key_file
        if remote_host_id is None:
            remote_host_id = create_new_id()
        self.id = remote_host_id

    def __str__(self):
        print "RemoteHost: ip_address={0}, port={1}, username={2}, secret_key_file={3}, id={4}"\
            .format(self.ip_address, self.port, self.username, self.secret_key_file, self.id)


class RemoteJob:
    def __init__(self, input_file, date, remote_host, remote_job_id=None):
        self.input_file = input_file
        self.date = date
        self.remote_host = remote_host
        if remote_job_id is None:
            remote_job_id = create_new_id()
        self.id = remote_job_id

    def __str__(self):
        print "RemoteJob: input_file={0}, date={1}, remote_host={2}, id={3}"\
            .format(self.input_file, self.date, str(self.remote_host), self.id)
