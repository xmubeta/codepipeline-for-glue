#!python3

# This is a Labmda script to create/update AWS Glue job.
# This code is modified from 
# https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/deploy-an-aws-glue-job-with-an-aws-codepipeline-ci-cd-pipeline.html

import json
import os
from os.path import join
import zipfile
import boto3

s3 = boto3.client('s3')
glue = boto3.client('glue')
pipeline = boto3.client('codepipeline')
codecommit = boto3.client('codecommit')


def lambda_handler(event, context):
    job = event['CodePipeline.job']
    try:
        data = job['data']

        print(json.dumps(event))

        input_artifacts = data['inputArtifacts']
        source_code_artifact = input_artifacts[0]

        artifact_bucket = source_code_artifact['location']['s3Location']['bucketName']
        artifact_key = source_code_artifact['location']['s3Location']['objectKey']

        commit_id = source_code_artifact['revision']

        #get CodeCommit respository name from Lambda Environment Variables
        repository_name = os.getenv('REPOSITORY_NAME')
        print('repository_name', repository_name)

        #get commit message to decide what to do.

        codecommit_resp = codecommit.get_commit(
            repositoryName=repository_name,
            commitId=commit_id
        )
        print('codecommit_resp', codecommit_resp)

        if "commit" in codecommit_resp:
            message = codecommit_resp['commit']['message']
            #define the format of commit message:  
            # 'create glue_job_name xxxxx'  to create a new Glue job
            # or 'update glue_job_name xxxx' to update an existing Glue job
            # Glue job name is identical to Glue script name here.

            cm_action = message.split(' ')[0]
            cm_job_name = message.split(' ')[1].strip('\n')


            if (cm_action != 'create' and cm_action != 'update'):
                print(f'action is {cm_action}, no more work.')
                pipeline.put_job_failure_result(
                        jobId=job['id'],
                        failureDetails={
                            'type': 'JobFailed',
                            'message': f'commit action is not correct - {cm_action}.'
                        }
                    )
                return 0

            source_zip_file =  '/tmp/source_code.zip'
            s3.download_file(
                artifact_bucket, artifact_key,source_zip_file)
            
            f_zip = zipfile.ZipFile(source_zip_file,'r')
            source_path = '/tmp/source_code'
            f_zip.extractall(source_path)
            
            #get S3 bucket name for storing Glue job script
            glue_artifact_bucket = os.getenv('GLUE_BUCKET')
            print('glue_artifact_bucket', glue_artifact_bucket)

            #get S3 bucket prefix name for storing Glue job script, no leading '/' and no ending '/'
            glue_artifact_prefix = os.getenv('GLUE_PREFIX')
            print('glue_artifact_prefix', glue_artifact_prefix)

            file_key = f'{glue_artifact_prefix}/{cm_job_name}/{cm_job_name}.py'

            #upload script to S3
            s3.upload_file(
                f"{source_path}/{cm_job_name}/{cm_job_name}.py",
                glue_artifact_bucket,
                Key=file_key
            )
            print(f'file upload to {glue_artifact_bucket}/{file_key}')            




            glue_job_name = cm_job_name
            print('glue_job_name:', glue_job_name)

            if (cm_action == 'create'):
                #start to create a Glue job from json configuration file.
                print(f"read configuration from file {source_path}/{cm_job_name}/{cm_job_name}.json")

                f_config = open(f"{source_path}/{cm_job_name}/{cm_job_name}.json")
                glue_config = json.load(f_config)

                create_job_resp = glue.create_job(
                    Name=glue_job_name,
                    Description=glue_config.get('Description',''),
                    Role=glue_config.get('Role'),
                    ExecutionProperty=glue_config.get('ExecutionProperty'),
                    Command=glue_config.get('Command'),
                    DefaultArguments=glue_config.get('DefaultArguments'),
                    MaxRetries=glue_config.get('MaxRetries',3),
                    Timeout=glue_config.get('Timeout',2880),
                    GlueVersion=glue_config.get('GlueVersion','3.0'),
                    NumberOfWorkers=glue_config.get('NumberOfWorkers',10),
                    WorkerType=glue_config.get('WorkerType','G.1X')
                )
                print('create_job_resp:', create_job_resp)


            elif (cm_action == 'update'):
                #start to update a Glue job from json configuration file.
                print(f"read configuration from file {source_path}/{cm_job_name}/{cm_job_name}.json")

                f_config = open(f"{source_path}/{cm_job_name}/{cm_job_name}.json")
                glue_config = json.load(f_config)

                update_job_resp = glue.update_job(
                    JobName=glue_job_name,
                    JobUpdate=glue_config
                )
                print('update_job_resp:', update_job_resp)
                
            else:
                print(f'action is {cm_action}, no more work.')




            print('create/update job successfully')
            pipeline.put_job_success_result(
                jobId=job['id'],
                outputVariables={
                    'glue_job_name':glue_job_name
                } )
        else:
            print(f'commit is not found {commit_id}')
            pipeline.put_job_failure_result(
                        jobId=job['id'],
                        failureDetails={
                            'type': 'JobFailed',
                            'message': f'commit is not found {commit_id}.'
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
