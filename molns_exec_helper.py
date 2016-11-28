#!/usr/bin/env python

# This program executes on a cluster.
import json
import pickle
import sys
import os
import logging


def run_job(logs, cluster_exec_input_file, cluster_exec_output_file, pickled_cluster_input_file, storage_dir=None,
            logger=None):
        try:
            lib_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sys.path.append(lib_path)

            import molnsutil

            with open(cluster_exec_input_file, "rb") as inp:
                inp_obj = pickle.load(inp)

            number_of_trajectories = inp_obj['number_of_trajectories']

            if inp_obj.get('add_realizations', False):
                ensemble = molnsutil.DistributedEnsemble(pickled_cluster_input_file=pickled_cluster_input_file,
                                                         qsub=True, storage_mode="Local", logger=logger)
                result = json.loads(ensemble.add_realizations(number_of_trajectories=number_of_trajectories))

                old_storage_dir = result["realizations_directory"]
                # Copy generated realizations from temporary directory to job directory.
                result["realizations_directory"] = molnsutil.utils.copy_generated_realizations_to_job_directory(
                    realizations_storage_directory=old_storage_dir,
                    store_realizations_dir=storage_dir)

                if len(os.listdir(result["realizations_directory"])) == 0:
                    from molnsutil.utils import jsonify
                    raise Exception(jsonify(logs="Something went wrong while generating realizations. "
                                    "Please check {0} for detailed logs.".format(old_storage_dir)))

                result = json.dumps(result)

            elif inp_obj.get('realizations_storage_directory', None) is not None:
                ensemble = molnsutil.DistributedEnsemble(pickled_cluster_input_file=pickled_cluster_input_file,
                                                         qsub=True, storage_mode="Local", logger=logger)
                mapped_results = ensemble.qsub_map_aggregate_stored_realizations(
                    pickled_cluster_input_file=pickled_cluster_input_file,
                    realizations_storage_directory=inp_obj.get('realizations_storage_directory'),
                    result_list=inp_obj.get("result_list"))
                result = ensemble.run_reducer(pickled_cluster_input_file=pickled_cluster_input_file,
                                              mapped_results=mapped_results)

            else:
                params = inp_obj['params']
                store_realizations = inp_obj['store_realizations']

                sweep = molnsutil.ParameterSweep(pickled_cluster_input_file=pickled_cluster_input_file,
                                                 parameters=params, qsub=True, logger=logger, storage_mode="Local")
                result = sweep.run(number_of_trajectories=number_of_trajectories,
                                   store_realizations=store_realizations, progress_bar=False,
                                   store_realizations_dir=storage_dir)

            with open(cluster_exec_output_file, "w") as out:
                out.write("{0}".format(result))

        except Exception as e:
            with open(logs, 'w') as stdout_fh:
                stdout_fh.write("\n\n ----------------------------------------------------------- \n\n {0}".format(e))


if __name__ == "__main__":
    base_job_dir = os.path.dirname(os.path.abspath(__file__))
    realizations_dir = os.path.join(base_job_dir, "realizations")

    if not os.path.exists(realizations_dir):
        os.mkdir(realizations_dir)

    with open(os.path.join(base_job_dir, "pid"), 'w+') as p:
        p.write(str(os.getpid()))

    log_file = os.path.join(base_job_dir, "molns_exec_helper_logs")

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("molns_exec_helper")
    logger.addHandler(logging.FileHandler(filename=log_file))
    logger.setLevel(logging.DEBUG)

    run_job(logs=log_file, cluster_exec_input_file=os.path.join(base_job_dir, "cluster-exec-input-file"),
            cluster_exec_output_file=os.path.join(base_job_dir, "cluster-exec-output-file"),
            storage_dir=realizations_dir, logger=logger,
            pickled_cluster_input_file=os.path.join(base_job_dir, "pickled-cluster-input-file"))

    with open(os.path.join(base_job_dir, "cluster-exec-job-complete"), 'w+') as comp:
        comp.write("Job completed.")
