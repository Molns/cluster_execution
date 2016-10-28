#!/usr/bin/env python
import traceback
import sys
import pickle
from molnsutil.molnsutil.molnsutil import ParameterSweep
from molnsutil.molnsutil import molns_cloudpickle


def run_job(stdout_file):
        with open(stdout_file, 'w') as stdout_fh:
            try:
                with open("cluster-exec-input-file") as inp:
                    unpickled_list = pickle.load(inp)

                model_cls = unpickled_list[0]
                params = unpickled_list[1]
                mapper = unpickled_list[2]
                aggregator = unpickled_list[3]
                reducer = unpickled_list[4]
                number_of_trajectories = unpickled_list[5]
                store_realizations = unpickled_list[6]

                sweep = ParameterSweep(model_class=model_cls, parameters=params, qsub=True, storage_mode="Local")
                result = sweep.run(mapper=mapper, aggregator=aggregator, reducer=reducer, number_of_trajectories=number_of_trajectories,
                                   store_realizations=store_realizations, progress_bar=False)

                with open("cluster-exec-output-file") as out:
                    molns_cloudpickle.dump(result, out)

            except Exception as e:
                stdout_fh.write('Error: {}'.format(str(e)))
                stdout_fh.write(traceback.format_exc())
                raise sys.exc_info()[1], None, sys.exc_info()[2]


if __name__ == "__main__":
    run_job("molns_exec_helper_stdout")
