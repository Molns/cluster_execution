#!/usr/bin/env python

# This program executes on a cluster.
import traceback
import pickle
import sys
import os


def run_job(logs, cluster_exec_input_file, cluster_exec_output_file, storage_dir=None):
        with open(logs, 'w') as stdout_fh:
            lib_path = ""
            try:
                lib_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                sys.path.append(lib_path)

                from molnsutil import molnsutil, molns_cloudpickle

                with open(cluster_exec_input_file, "rb") as inp:
                    inp_obj = pickle.load(inp)

                model_cls = inp_obj['model_cls']
                params = inp_obj['params']
                mapper = inp_obj['mapper']
                aggregator = inp_obj['aggregator']
                reducer = inp_obj['reducer']
                number_of_trajectories = inp_obj['number_of_trajectories']
                store_realizations = inp_obj['store_realizations']

                sweep = molnsutil.ParameterSweep(model_class=model_cls, parameters=params, qsub=True,
                                                 storage_mode="Local")
                result = sweep.run(mapper=mapper, aggregator=aggregator, reducer=reducer,
                                   number_of_trajectories=number_of_trajectories, store_realizations=store_realizations,
                                   progress_bar=False, store_realizations_dir=storage_dir)

                with open(cluster_exec_output_file, "wb") as out:
                    molns_cloudpickle.dump(result, out)

            except Exception as e:
                stdout_fh.write('Error: {0}\nLib path: {1}\nstorage_dir={2}\n'.format(str(e), lib_path, storage_dir))
                stdout_fh.write(traceback.format_exc())


if __name__ == "__main__":
    base_job_dir = os.path.dirname(os.path.abspath(__file__))
    realizations_dir = os.path.join(base_job_dir, "realizations")

    if not os.path.exists(realizations_dir):
        os.mkdir(realizations_dir)

    with open(os.path.join(base_job_dir, "pid"), 'w+') as p:
        p.write(str(os.getpid()))

    run_job(logs=os.path.join(base_job_dir, "molns_exec_helper_logs"),
            cluster_exec_input_file=os.path.join(base_job_dir, "cluster-exec-input-file"),
            cluster_exec_output_file=os.path.join(base_job_dir, "cluster-exec-output-file"),
            storage_dir=realizations_dir)

    with open(os.path.join(base_job_dir, "cluster-exec-job-complete"), 'w+') as comp:
        comp.write("Job completed.")
