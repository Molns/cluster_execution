def create_pickled_cluster_input_file(storage_path, mapper=None, aggregator=None, reducer=None, model_class=None):
    import os
    from molnsutil import molns_cloudpickle
    if model_class is None and mapper is None and aggregator is None and reducer is None:
        return None

    if os.access(storage_path, os.W_OK) is False:
        raise IOError("{0} is not writable.".format(storage_path))

    unpickled_list = dict(model_class=model_class, mapper=mapper, aggregator=aggregator, reducer=reducer)

    with open(storage_path, "wb") as input_file:
        molns_cloudpickle.dump(unpickled_list, input_file)


class Log:
    verbose = True

    def __init__(self):
        pass

    @staticmethod
    def write_log(message):
        if Log.verbose:
            print message
