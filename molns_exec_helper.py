#!/usr/bin/env python

# This program executes on a cluster.
import traceback
import pickle
import sys
import os


def run_job(stdout_file):
        with open(stdout_file, 'w') as stdout_fh:
            try:
                sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                from molnsutil import molnsutil, molns_cloudpickle
                with open("cluster-exec-input-file") as inp:
                    unpickled_list = pickle.load(inp)

                model_cls = unpickled_list[0]
                params = unpickled_list[1]
                mapper = unpickled_list[2]
                aggregator = unpickled_list[3]
                reducer = unpickled_list[4]
                number_of_trajectories = unpickled_list[5]
                store_realizations = unpickled_list[6]

                sweep = molnsutil.ParameterSweep(model_class=model_cls, parameters=params, qsub=True,
                                                 storage_mode="Local")
                result = sweep.run(mapper=mapper, aggregator=aggregator, reducer=reducer,
                                   number_of_trajectories=number_of_trajectories, store_realizations=store_realizations,
                                   progress_bar=False)

                with open("cluster-exec-output-file") as out:
                    molns_cloudpickle.dump(result, out)

            except Exception as e:
                stdout_fh.write('Error: {}'.format(str(e)))
                stdout_fh.write(traceback.format_exc())


if __name__ == "__main__":
    with open("pid") as p:
        p.write(str(os.getpid()))

    run_job("molns_exec_helper_logs")

    with open("cluster-exec-job-complete") as comp:
        comp.write("Job completed.")
