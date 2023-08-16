## Athena Metrics
You can use this code to gather custom metrics from Athena API, that are not available in cloudtrail.

The athena_util.py is a generic class that can be used to interact with Athena API and can be used outside this repository

Step1 . Clone the repo to your local 
Step 2.  Generate a wheel file
pip install wheel
python setup.py bdist_wheel
Step 3 : Upload package to your s3 
    https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html
Step 4. Create a glue python shell job and call the API 