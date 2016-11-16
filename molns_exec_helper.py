#!/usr/bin/env python

# This program executes on a cluster.
import traceback
import pickle
import sys
import os


def run_job(logs, cluster_exec_input_file, cluster_exec_output_file, pickled_cluster_input_file, storage_dir=None):
        with open(logs, 'w') as stdout_fh:
            lib_path = ""
            try:
                lib_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                sys.path.append(lib_path)

                import molnsutil

                with open(cluster_exec_input_file, "rb") as inp:
                    inp_obj = pickle.load(inp)

                number_of_trajectories = inp_obj['number_of_trajectories']

                if inp_obj.get('add_realizations', False):
                    ensemble = molnsutil.DistributedEnsemble(pickled_cluster_input_file=pickled_cluster_input_file,
                                                             qsub=True, storage_mode="Local")
                    import json
                    result = json.loads(ensemble.add_realizations(number_of_trajectories=number_of_trajectories))

                    old_storage_dir = result['realizations_directory']
                    # Copy generated realizations from temporary directory to job directory.
                    result['realizations_directory'] = molnsutil.utils.copy_generated_realizations_to_job_directory(
                        realizations_storage_directory=old_storage_dir,
                        store_realizations_dir=storage_dir)

                    if len(os.listdir(result['realizations_directory'])) == 0:
                        raise Exception("Something went wrong while generating realizations. "
                                        "Please check {0} for detailed logs.".format(old_storage_dir))

                elif inp_obj.get('realizations_storage_directory', False):
                    ensemble = molnsutil.DistributedEnsemble(pickled_cluster_input_file=pickled_cluster_input_file,
                                                             qsub=True, storage_mode="Local")
                    mapped_results = ensemble.qsub_map_aggregate_stored_realizations(
                        pickled_cluster_input_file=pickled_cluster_input_file,
                        realizations_storage_directory=inp_obj.get('realizations_storage_dir'))
                    result = ensemble.run_reducer(pickled_cluster_input_file=pickled_cluster_input_file,
                                                  mapped_results=mapped_results)

                else:
                    params = inp_obj['params']
                    store_realizations = inp_obj['store_realizations']

                    sweep = molnsutil.ParameterSweep(pickled_cluster_input_file=pickled_cluster_input_file,
                                                     parameters=params, qsub=True,
                                                     storage_mode="Local")
                    result = sweep.run(number_of_trajectories=number_of_trajectories,
                                       store_realizations=store_realizations, progress_bar=False,
                                       store_realizations_dir=storage_dir)

                with open(cluster_exec_output_file, "w") as out:
                    out.write("{0}".format(result))

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
            storage_dir=realizations_dir,
            pickled_cluster_input_file=os.path.join(base_job_dir, "pickled-cluster-input-file"))

    with open(os.path.join(base_job_dir, "cluster-exec-job-complete"), 'w+') as comp:
        comp.write("Job completed.")
