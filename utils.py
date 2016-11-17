def create_pickled_cluster_input_file(storage_path, mapper=None, aggregator=None, reducer=None, model_class=None):

    from molnsutil import molns_cloudpickle

    if model_class is None and mapper is None and aggregator is None and reducer is None:
        return None

    unpickled_list = dict(model_class=molns_cloudpickle.dumps(model_class), mapper=mapper, aggregator=aggregator,
                          reducer=reducer)

    with open(storage_path, "wb") as input_file:
        molns_cloudpickle.dump(unpickled_list, input_file)


def format_result_list_dict(result_list):
    for key in result_list.keys():
        result_list[int(key)] = result_list[key]
        result_list.pop(key)

    return result_list


class Log:
    verbose = True

    def __init__(self):
        pass

    @staticmethod
    def write_log(message):
        if Log.verbose:
            print message
