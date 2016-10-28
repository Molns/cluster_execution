def get_files_to_transfer(remote_job):
    import os
    import constants
    files_to_transfer = [os.path.join(os.path.dirname(os.path.abspath(__file__)), constants.MolnsExecHelper),
                         remote_job.input_file]
    return files_to_transfer


class Log:
    verbose = True

    def __init__(self):
        pass

    @staticmethod
    def write_log(message):
        if Log.verbose:
            print message
