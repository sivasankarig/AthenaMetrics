## Athena Metrics
You can use this code to gather custom metrics from Athena API, that are not available in cloudtrail.

The athena_util.py is a generic class that can be used to interact with Athena API and can be used outside this repository

Step1 . Clone the repo to your local 
Step 2.  Generate a wheel file
pip install wheel
python setup.py bdist_wheel
Step 3 : Upload package to your s3 
    https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html
Step 4. Create a cloudtrail table 

CREATE EXTERNAL TABLE cloudtrail_logs_pp(
    eventVersion STRING,
    userIdentity STRUCT<
        type: STRING,
        principalId: STRING,
        arn: STRING,
        accountId: STRING,
        invokedBy: STRING,
        accessKeyId: STRING,
        userName: STRING,
        sessionContext: STRUCT<
            attributes: STRUCT<
                mfaAuthenticated: STRING,
                creationDate: STRING>,
            sessionIssuer: STRUCT<
                type: STRING,
                principalId: STRING,
                arn: STRING,
                accountId: STRING,
                userName: STRING>,
            ec2RoleDelivery:string,
            webIdFederationData:map<string,string>
        >
    >,
    eventTime STRING,
    eventSource STRING,
    eventName STRING,
    awsRegion STRING,
    sourceIpAddress STRING,
    userAgent STRING,
    errorCode STRING,
    errorMessage STRING,
    requestparameters STRING,
    responseelements STRING,
    additionaleventdata STRING,
    requestId STRING,
    eventId STRING,
    readOnly STRING,
    resources ARRAY<STRUCT<
        arn: STRING,
        accountId: STRING,
        type: STRING>>,
    eventType STRING,
    apiVersion STRING,
    recipientAccountId STRING,
    serviceEventDetails STRING,
    sharedEventID STRING,
    vpcendpointid STRING,
    tlsDetails struct<
        tlsVersion:string,
        cipherSuite:string,
        clientProvidedHostHeader:string>
  )
PARTITIONED BY (
   `day` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://bucket/AWSLogs/account-id/CloudTrail/aws-region'
TBLPROPERTIES (
  'projection.enabled'='true', 
  'projection.day.format'='yyyy/MM/dd', 
  'projection.day.interval'='1', 
  'projection.day.interval.unit'='DAYS', 
  'projection.day.range'='2020/01/01,NOW', 
  'projection.day.type'='date', 
  'storage.location.template'='s3://bucket/AWSLogs/account-id/CloudTrail/aws-region/${day}')

https://docs.aws.amazon.com/athena/latest/ug/cloudtrail-logs.html#create-cloudtrail-table-partition-projection
Step 4. Create a glue python shell job and execute it
Step 5. Create api_output table and load partitions
CREATE EXTERNAL TABLE IF NOT EXISTS athena_api_output(
  queryid string,
  querydatabase string,
  executiontime bigint,
  datascanned bigint,
  status string,
  submissiondatetime string,
  completiondatetime string
  )
  PARTITIONED BY (
   dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://<s3 location of the output from the API calls>'
TBLPROPERTIES ('has_encrypted_data'='false')

