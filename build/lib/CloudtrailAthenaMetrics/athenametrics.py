from .athena_util import AthenaUtil
import argparse
import boto3
import datetime
import sys
import logging
import sys
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


'''
Collect Athena query IDs from cloudtrail and
query the Athena API for those query IDs
'''

def parse_arguments():
  
  parser = argparse.ArgumentParser(
                  description='Collect Athena data usage metrics')
  parser.add_argument('-s', '--staging-folder',
                        help='staging folder for Athena',
                        required=True)
  parser.add_argument('-d', '--destination-bucket',
                        help='an existing bucket where the metrics are saved.',
                        required=True)
  parser.add_argument('-t', '--tablename',
                        help='a cloudtrail table name with database',
                        required=False,default='default."cloudtrail_logs_pp"')
  parser.add_argument('-r', '--region',
                        help='an query to extract ids.',
                        required=False,
                        default='us-east-1')
  parser.add_argument('-b', '--backfill',
                        help='an backfill for last month',
                        required=False,
                        default=False)
  return parser



def splitarr(array, chunksize):
  ''' Split the array in sub arrays of size chunksize.
  return the array of these sub-arrays.
  '''
  ret = []
  lower = 0
  for j in range(lower, len(array)):
    tmp = []
    upper = lower + chunksize
    if( upper >= len(array) ):
      upper = len(array)
    for k in range(lower,upper):
      tmp.append(array[k])
    lower = upper
    ret.append(tmp)
    if( upper >= len(array) ) :
      return ret


def main():
  parser = parse_arguments()
  if __name__ == '__main__':
    args = parser.parse_args()
    collect_metrics( args.staging_folder, args.destination_bucket,args.tablename,args.region,args.backfill)


def collect_metrics(staging_folder, destination_bucket,tablename='default.cloudtrail_logs_pp',region='us-east-1',backfill=False):
  ''' Query Cloudtrail to extract queryIDs and iterate the queryIDs
      in batches of 50. For each queryID, invoke the Athena API to
      get athena usage information.
  '''
  client = boto3.client('athena', region_name=region)

  numberofdays= -44 if backfill else -1

  ## The Athena query on cloudtrails table to get last days' query IDs
  
  query_str = f"""
 with data as
  (SELECT json_extract(responseelements, '$.queryExecutionId') as query_id,
          (useridentity.arn) as uid,
          (useridentity.sessioncontext.sessionIssuer.userName) as role,
          day
  FROM {tablename}
  WHERE eventsource='athena.amazonaws.com'
  AND eventname='StartQueryExecution' 
  AND json_extract(responseelements, '$.queryExecutionId') is not null),
  queryid_seq as (SELECT query_id,cast(date_parse(day, '%Y/%m/%d') as date) dt, row_number() over(partition by day) seq FROM data
  WHERE date_parse(day, '%Y/%m/%d') >  date_add('day',{numberofdays},now())
  ),
  batch_num as (select query_id,dt,seq/40 batch from queryid_seq)
  select dt,batch,array_agg(query_id)  query_ids from batch_num group by 1,2
  """

  logger.info(" query String {}".format(query_str))
  
  result = AthenaUtil(s3_staging_folder = staging_folder,region_name=region) \
              .execute_query(query_str)

  query_ids =[]
  for row in result["ResultSet"]["Rows"]:
      day_data = row['Data'][0]
      batch_data = row['Data'][1]
      query_data = row['Data'][2]
      query_ids = json.loads(query_data['VarCharValue'].strip('"'))
      day = day_data['VarCharValue'].strip('"')
      batch_num = day_data['batch_data'].strip('"')
      i = 0
      ## Iterate in batches of 50. That is the default Athena limit per account.
      try:
          response = client.batch_get_query_execution(
          QueryExecutionIds=query_ids
          )
          athena_metrics = ""
          for row in response["QueryExecutions"]:
              queryid = row['QueryExecutionId']
              querydatabase = "null"
              if 'QueryExecutionContext' in row and 'Database' in row['QueryExecutionContext']:
                  querydatabase = row['QueryExecutionContext']['Database']
              executiontime = "null"
              if 'EngineExecutionTimeInMillis' in row['Statistics']:
                  executiontime = str(row['Statistics']['EngineExecutionTimeInMillis'])
              datascanned = "null"
              if 'DataScannedInBytes' in row['Statistics']:
                  datascanned = str(row ['Statistics']['DataScannedInBytes'])
              status = row ['Status']['State']
              submissiondatetime="null"
              if 'SubmissionDateTime' in row['Status']:
                  submissiondatetime = str(row['Status']['SubmissionDateTime'])
              completiondatetime = "null"
              if 'CompletionDateTime' in row['Status']:
                  completiondatetime = str(row['Status']['CompletionDateTime'])
              athena_metrics += ','.join([queryid,querydatabase,executiontime,datascanned,status,submissiondatetime,completiondatetime])+'\n'
      except Exception as e:
          print (e)

      sys.stdout=open("out" + str(i) + ".csv","w")
      print (athena_metrics)
      sys.stdout.close()

      s3 = boto3.resource('s3', region_name='us-west-2')

      infile = 'out' + str(i) + '.csv'
      outfile = 'athena-metrics'+'/dt='+str(day)+'/'+'out' + str(batch_num) + '.csv'
      s3.meta.client.upload_file(infile, destination_bucket, outfile)

      i = i + 1

if __name__ == "__main__":
  main()
