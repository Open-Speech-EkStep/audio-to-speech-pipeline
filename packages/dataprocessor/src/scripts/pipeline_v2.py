import os

import yaml

from .remote_pipeline import RemoteAudioPipeline

yaml.warnings({'YAMLLoadWarning': False})
import sys
from .gcs_operations import CloudStorageOperations

if __name__ == "__main__":

    # Get Arguments
    print("Fetching Arguments...")

    if len(sys.argv) > 6:
        job_mode = sys.argv[1]  # local,cluster
        gcs_bucket_name = sys.argv[2]  # remote_gcs bucket name
        config_path = sys.argv[3]  # remote gcs path, for local it will be src/resources/local/config_local.yaml
        data_source = sys.argv[4]  # audio source: joshtalks, brahmakumari
        audio_id = sys.argv[5]  # unique identifier for each audio file
        audio_extn = sys.argv[6]  # audio file exten: can be .mp4 or .mp3
    else:
        print("Required Arguements are not passed correctly.Please retry.")
        exit()

    print("Arguments Received...")
    print("Arg 1 : job_mode : {}".format(job_mode))
    print("Arg 2 : gcs_bucket_name: {}".format(gcs_bucket_name))
    print("Arg 3 : config_path: {}".format(config_path))
    print("Arg 4 : data_source: {}".format(data_source))
    print("Arg 5 : audio_id: {}".format(audio_id))
    print("Arg 6 : audio_extn: {}".format(audio_extn))

    current_working_directory = os.getcwd()
    config_local_path = os.path.join(current_working_directory, "src/resources/" + job_mode + "/config.yaml")
    if (job_mode == "cluster"):
        # Download config file from GCS
        print("Downloading config file from cloud storage to local")
        obj_gcs = CloudStorageOperations()
        obj_gcs.download_to_local(bucket_name=gcs_bucket_name,
                                  source_blob_name=config_path,
                                  destination=config_local_path,
                                  is_directory=False)
        print("download successful")
        obj = RemoteAudioPipeline()
        obj.fit(config_local_path, gcs_bucket_name, data_source, audio_id, audio_extn, job_mode=job_mode)
