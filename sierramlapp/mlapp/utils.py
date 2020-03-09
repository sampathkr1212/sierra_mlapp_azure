import os
import time
import json
import urllib.request
from flask import url_for, current_app
from azure.storage.blob import *
from werkzeug.utils import secure_filename

def save_file(data_file):
    data_file_path = os.path.join(current_app.root_path, 'static/data_files')
    data_file.stream.seek(0)
    data_file.save(os.path.join(data_file_path, secure_filename(data_file.filename)))


def printHttpError(httpError):
    print("The request failed with status code: " + str(httpError.code))

    # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
    print(httpError.info())

    print(json.loads(httpError.read().decode("utf8", 'ignore')))
    return

def saveBlobToFile(blobUrl, resultsLabel):
    output_file = os.path.join(current_app.root_path, 'static/data_files/predicted_result.csv') # Replace this with the location you would like to use for your output file, and valid file extension (usually .csv for scoring results, or .ilearner for trained models)
    print("Reading the result from " + blobUrl)
    global result_download_link
    result_download_link = blobUrl
    try:
        response = urllib.request.urlopen(blobUrl)
    except urllib.error.HTTPError as error:
        printHttpError(error)
        return

    with open(output_file, "w+", encoding='utf8') as f:
        f.write(response.read().decode("utf8", 'ignore'))
    print(resultsLabel + " have been written to the file " + output_file)
    return

def processResults(result):
    first = True
    results = result["Results"]
    for outputName in results:
        result_blob_location = results[outputName]
        sas_token = result_blob_location["SasBlobToken"]
        base_url = result_blob_location["BaseLocation"]
        relative_url = result_blob_location["RelativeLocation"]

        print("The results for " + outputName + " are available at the following Azure Storage location:")
        print("BaseLocation: " + base_url)
        print("RelativeLocation: " + relative_url)
        print("SasBlobToken: " + sas_token)

        if (first):
            first = False
            url3 = base_url + relative_url + sas_token
            saveBlobToFile(url3, "The results for " + outputName)
    return

def uploadFileToBlob(input_file, input_blob_name, storage_container_name, storage_account_name, storage_account_key):
    blob_service = BlockBlobService(account_name=storage_account_name, account_key=storage_account_key)

    print("Uploading the input to blob storage...")
    blob_service.create_blob_from_path(storage_container_name, input_blob_name, input_file)

def invokeBatchExecutionService(excel_filename):
    storage_account_name = "mlstudionewcheck" # Replace this with your Azure Storage Account name
    storage_account_key = "Rwu+6gnnZEqxq8tbPH7jERu0BnwwEgsrYIMCf9mrfHJnmEtpjdqQ+UpPk4GbJvKESzXBS7JsKvQfinPUc5YeeQ==" # Replace this with your Azure Storage Key
    storage_container_name = "newcheckcontainer" # Replace this with your Azure Storage Container name
    connection_string = "DefaultEndpointsProtocol=https;AccountName=" + storage_account_name + ";AccountKey=" + storage_account_key

    api_key = "qD44SB1Kqxy0lW+Ru9dTklCaW+QkQFe8uBBGfoVcZz4qXVa64eN6Zo91tEJPL5iBMpQWmc4pCp5rt14fLa9iSg==" # Replace this with the API key for the web service
    url = "https://ussouthcentral.services.azureml.net/workspaces/7eef30c99d7046829d68a1a3966b62b9/services/438ac507b54843438a501ac8c91658a3/jobs"
    uploadFileToBlob(os.path.join(current_app.root_path, 'static/data_files/' + excel_filename), "Mercari_input_Test_Data.csv", storage_container_name, storage_account_name, storage_account_key);

    payload = {
            "Inputs": {
                    "input1":
                    {
                        "ConnectionString": connection_string,
                        "RelativeLocation": "/" + storage_container_name + "/Mercari_input_Test_Data.csv"
                    },
            },

            "Outputs": {
                    "output1":
                    {
                        "ConnectionString": connection_string,
                        "RelativeLocation": "/" + storage_container_name + "/predicted_result.csv" # Replace this with the location you would like to use for your output file, and valid file extension (usually .csv for scoring results, or .ilearner for trained models)
                    },
            },

        "GlobalParameters": {
        }
    }

    body = str.encode(json.dumps(payload))
    headers = { "Content-Type":"application/json", "Authorization":("Bearer " + api_key)}
    print("Submitting the job...")

    # submit the job
    req = urllib.request.Request(url + "?api-version=2.0", body, headers)

    try:
        response = urllib.request.urlopen(req)
    except urllib.error.HTTPError as error:
        printHttpError(error)
        return

    result = response.read()
    job_id = result.decode("utf8", 'ignore')[1:-1]
    print("Job ID: " + job_id)

    # start the job
    print("Starting the job...")
    body = str.encode(json.dumps({}))
    req = urllib.request.Request(url + "/" + job_id + "/start?api-version=2.0", body, headers)
    try:
        response = urllib.request.urlopen(req)
    except urllib.error.HTTPError as error:
        printHttpError(error)
        return

    url2 = url + "/" + job_id + "?api-version=2.0"

    while True:
        print("Checking the job status...")
        req = urllib.request.Request(url2, headers = { "Authorization":("Bearer " + api_key) })

        try:
            response = urllib.request.urlopen(req)
        except urllib.error.HTTPError as error:
            printHttpError(error)
            return

        result = json.loads(response.read().decode("utf8", 'ignore'))
        status = result["StatusCode"]
        if (status == 0 or status == "NotStarted"):
            print("Job " + job_id + " not yet started...")
        elif (status == 1 or status == "Running"):
            print("Job " + job_id + " running...")
        elif (status == 2 or status == "Failed"):
            print("Job " + job_id + " failed!")
            print("Error details: " + result["Details"])
            break
        elif (status == 3 or status == "Cancelled"):
            print("Job " + job_id + " cancelled!")
            break
        elif (status == 4 or status == "Finished"):
            print("Job " + job_id + " finished!")
            processResults(result)
            break
        time.sleep(1) # wait one second
    return result_download_link
