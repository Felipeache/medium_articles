"""
requirements:

google-api-client==3.14.159265359.post1
google-cloud-storage==1.36.1
oauth2client==4.1.3
"""
from googleapiclient.discovery import build
from os import getenv
from requests import post
import logging

project = getenv("GCP_PROJECT")
region = getenv("FUNCTION_REGION")


def get_jobs_list():
  """List all the Dataflow jobs that are runing rigth now""" 

    service = build("dataflow", "v1b3", cache_discovery=False)
    response = (
        service.projects().locations().jobs().list(projectId=project, location=region)
    )
    jobs_list = response.execute()
    return jobs_list["jobs"] if jobs_list else False


def is_runing(job_name):
  """Determines if another job with the same name is running to avoid to recreate a second one"""
    # True if there is a running job or
    # another job has the same name
    jobs_list = get_jobs_list()
    already_runing = False
    try:
        number_of_jobs = len(jobs_list)
        i = 0
        while not already_runing:
            if job_name == jobs_list[i]["name"]:
                already_runing = True
            else:
                if i + 1 < number_of_jobs:
                    i += 1
                else:
                    break
        return already_runing
    except TypeError:
        return False


def get_all_blobs(bucket, prefix):
  """list all the files in the bucket""" 
    import google.cloud.storage.client as gcs

    storage_client = gcs.Client()
    bucket = storage_client.get_bucket(bucket)
    shards_list = list(bucket.list_blobs(prefix=prefix))
    return str(shards_list).split(",")[-2]

def main(data, context):
  """Entry point of the Cloud Function"""
  
    from datetime import datetime

    now = datetime.now().strftime("%d-%m-%Y-%H-%M")
    job_name = f"clean-file-{now}"
    bucket = data["bucket"]
    blob = data["name"]
    prefix = "prefix"
    logging.info(f"Cloud function detected a new file named {blob}.")
    blob_name = get_all_blobs(bucket, prefix)  # blob #
    temp_location = f"gs://{project}-processing/dataflow/temp"
    inputFile = f"gs://{bucket}/{blob_name}"
    scope = "Staging" if "staging" in project else "Prod"
    """This job_params is creating all the specs to send to the Dataflow job"""
    job_params = {
                              'jobName': job_name,
                              'parameters': {
                                  #Self defined params that will be used in Dataflow
                                  "origin_path": f'gs://{bucket}',
                                  "blob_name": blob,
                                  "working_bucket": project.strip(),
                              },
                              "environment": {
                                "tempLocation": temp_location.strip(),
                                "stagingLocation":f"gs://{project}-processing/dataflow/staging/",
                                "zone": "europe-west1-b",
                                "serviceAccountEmail": f"this_is_a_sa@{project}.iam.gserviceaccount.com",
                                "network": f'{project}',
                                "subnetwork": f'https://www.googleapis.com/compute/v1/projects/{project}/regions/europe-west1/subnetworks/{project}-01', 
                },
                              'containerSpecGcsPath': f"gs://{project}-scripts/flex-template-metadata/dataflow-jaffa-metadata.json"
                            }
    if "FILE_NAME_THAT_I_NEED" in blob:
        logging.info(f"{blob} is the file. A Dataflow job will start now")
        if not is_runing(job_name):
            dataflow = build("dataflow", "v1b3", cache_discovery=False)
            #starting the Dataflow job ==>
            dfrequest = (
                dataflow.projects().locations().flexTemplates().launch(
                    projectId=project, location="europe-west1", body={
                            'launch_parameter': job_params
                        }
                )
            )
            dfrequest.execute()
            logging.info(f"Dataflow job: {job_name} is now running")
            
    else:
            wally_message = f":warning: Hello Human,\n\nThe unknown file: *{blob}* just dropped into bucket: *{bucket}*.\n\nI wont do anything.\n\nLove from Wally :robot-nerd:"
            logging.info(wally_message)
            
