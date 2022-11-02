#!python3

# This is a Labmda script to run AWS Glue job.
# This code is modified from 
# https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/deploy-an-aws-glue-job-with-an-aws-codepipeline-ci-cd-pipeline.html

import json

from os.path import join
import zipfile
import boto3

s3 = boto3.client('s3')
glue = boto3.client('glue')
pipeline = boto3.client('codepipeline')
#codecommit = boto3.client('codecommit')


def lambda_handler(event, context):
    job = event['CodePipeline.job']
    try:
        data = job['data']

        print(json.dumps(event))

        config = data['actionConfiguration']['configuration']
        glue_job_name = config['UserParameters']


        input_artifacts = data['inputArtifacts']
        source_code_artifact = input_artifacts[0]

        artifact_bucket = source_code_artifact['location']['s3Location']['bucketName']
        artifact_key = source_code_artifact['location']['s3Location']['objectKey']

        #get glue job configuration from artifacts to start
    
        source_zip_file =  '/tmp/source_code.zip'
        s3.download_file(
            artifact_bucket, artifact_key,source_zip_file)
        
        f_zip = zipfile.ZipFile(source_zip_file,'r')
        source_path = '/tmp/source_code'
        f_zip.extractall(source_path)

        cm_job_name = glue_job_name
        print(f"read configuration from file {source_path}/{cm_job_name}/{cm_job_name}.json")

        f_config = open(f"{source_path}/{cm_job_name}/{cm_job_name}.json")
        glue_config = json.load(f_config)

        #start a Glue job
        start_job_resp = glue.start_job_run(
            JobName=glue_job_name,
            Timeout=glue_config.get('Timeout',2880),
            NumberOfWorkers=glue_config.get('NumberOfWorkers',10),
            WorkerType=glue_config.get('WorkerType','G.1X')
        )
        print('start_job_resp:', start_job_resp)


        if ('JobRunId' in start_job_resp):
            print('start job successfully')
            pipeline.put_job_success_result(
                jobId=job['id'],
                outputVariables={
                    'glue_job_id':start_job_resp['JobRunId']
                } )
        else:
            print('fail to start job')
            pipeline.put_job_failure_result(
                jobId=job['id'],
                failureDetails={
                'type': 'JobFailed',
                'message': start_job_resp
                } 
            )


        

    except Exception as e:
        print('submitting unsuccessful job: ' + str(e))
        pipeline.put_job_failure_result(
            jobId=job['id'],
            failureDetails={
                'type': 'JobFailed',
                'message': str(e)
            }
        )
