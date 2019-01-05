### Invoking the Lambda Function

The Lambda function has been designed so that every time a file is uploaded in the input S3 bucket, the lambda function is triggered and it does the processing on the input file. 
The Lambda console in AWS allows for addition of things known as Triggers - which consist of various AWS services, uploads/changes to which will act as trigger and start the Lambda function/code.
The Lambda Console is shown below:

<p align="center">
  <img src="https://github.com/CSCI5253-Fall2018/project-3-serverless-anand-ramakrishnan/blob/master/Screenshots/LambdaConsole.PNG" title="Lambda Console">
</p>

The name of the Lambda role is myLambdaProject3. As the figure shows, the LHS shows the list of triggers to the function, and S3 is displayed. The RHS shows the AWS resources that the role has access to. This means that the services listed (S3, CloudWatch) can be accessed by the Lambda function. This is further detailed in the Lambda Integration into the AWS Framework part. 

When a Lambda role is created and S3 is set as trigger for it, we need to create a Trigger Event (the Trigger needs to be configured) that will actually decide when the Lambda function is going to be called. (which bucket, which action to the bucket)
The trigger event is shown below:

<p align="center">
  <img src="https://github.com/CSCI5253-Fall2018/project-3-serverless-anand-ramakrishnan/blob/master/Screenshots/S3TriggerEvent.PNG" title="S3 Trigger Event">
</p>

Here, the (input) bucket name has been specified, and the Event Type has been selected as 'ObjectCreated'. This ensures that whenever a new object is created (file uploaded) in the specified input bucket, the lambda function is triggered and becomes operational on that particular file. The Suffix (.txt) ensures that the trigger is only when text files are uploaded, this is to handle errors (uploading files of other types will not result in the lambda function being triggered).

### How the lambda function works

The Lambda function, after being triggered, runs the lambda_function.py code on the input file whose upload (automated - using a python script run from the command line) triggered it. The code ensures that the file content is processed as required and is uploaded (having the same name) into a target bucket. Every time/when each of the 2000 text files are uploaded, the lambda function runs and does the processing. Thus, at the end of the upload of the 2000th tweet into the input bucket, we see that the target bucket is filled with 2000 processed text files named 1.txt to 2000.txt. The difference in timestamps between the upload of 1.txt and 2000.txt into the target bucket by the lambda function is roughly 35 minutes - the time it took to run the python code for upload of files (in three batches - 600 in 10 minutes (1 each second), 200 then and 1200 in 20 minutes (1 each second)). 

This can be seen in the following screenshots:

<p align="center">
  <img src="https://github.com/CSCI5253-Fall2018/project-3-serverless-anand-ramakrishnan/blob/master/Screenshots/Processed Tweet 1 Timestamp.PNG" title="Tweet 1 Timestamp">
</p>

<p> </p>
<p></p>

<p align="center">
  <img src="https://github.com/CSCI5253-Fall2018/project-3-serverless-anand-ramakrishnan/blob/master/Screenshots/Processed Tweet 2000 Timestamp.PNG" title="Tweet 2000 Timestamp">
</p>

### What the Lambda function does

The Lambda function lambda_function.py that is run when the Lambda role is triggered, does the processing of the input file and places it into a target bucket. 

The required libraries are first imported. Lambda has an extremely nice feature wherein almost all Python libraries are in-built, and one needs to just import the required libraries for direct use.
Boto3 library allows use of/integration of S3 with Lambda, and a client and resource are created. The client is used to read the file and do the processing, the resource uploads the new file to the target bucket. 
A lambda handler function that does all the work is defined. In this, the target bucket is specified, and the input bucket and key (name of the file uploaded) are also identified. The input file is then read and decoded in UTF-8 format. A list of stop-words have been listed, after which processing is done (non-alphabets removed, all letters converted to lowercase, stop-words removed). The result obtained is comma separated (csv) format, which is then converted into a string with spaces in between words. This string is then uploaded in the target bucket with its name as 'key' (the same name it had when being uploaded into the input bucket).

### Lambda Integration into the AWS Framework

This part describes how the Lambda role and function are integrated with various AWS services. On the Lambda Console, we saw that in the RHS, AWS resources that the lambda role can access were listed. This includes S3 and CloudWatch. 

In order to work, it is essential that the lambda function has permission to access the S3 Buckets (to read from input bucket and write to output bucket). This is done by creating a role (in IAM - project3-lambda-role) that allows Lambda full access to S3 resources. (S3FullAccess is the policy name). 

CloudWatch is a monitoring service offered by AWS that allows its resources to be monitored. In this case, CloudWatch is incorporated to monitor Lambda function (each time it is triggered and run). This permission for lambda to access cloudwatch is given by another policy attached to the same role (AWSLambdaExecute) that includes it.

When the lambda function is created, the role is specified, as shown below. 

<p align="center">
  <img src="https://github.com/CSCI5253-Fall2018/project-3-serverless-anand-ramakrishnan/blob/master/Screenshots/LambdaRole.PNG" title="Selecting Lambda Role">
</p>

The two policies attached to the role are shown below.

<p align="center">
  <img src="https://github.com/CSCI5253-Fall2018/project-3-serverless-anand-ramakrishnan/blob/master/Screenshots/RolePolicies.PNG" title="Selecting Policies attached to the Role">
</p>

The description of the policies is shown below.

<p align="center">
  <img src="https://github.com/CSCI5253-Fall2018/project-3-serverless-anand-ramakrishnan/blob/master/Screenshots/PolicyDetails.PNG" title="Policy Details">
</p>

We see that S3FullAccess gives Full Access to S3 resources, and AWSLambdaExecute gives read,write and list permissions for CloudWatch Logs (and read,write permissions for S3, but an S3 policy was attached in any case just to be safe).

Lastly, when the lambda function executes, we can go to the CloudWatch page and see that logs have been created with timestamps. (showing last event time)

<p align="center">
  <img src="https://github.com/CSCI5253-Fall2018/project-3-serverless-anand-ramakrishnan/blob/master/Screenshots/CloudWatch Logs.PNG" title="CloudWatch Logs">
</p>

When we click on a log, we can see details (START, END and REPORT) which shows that the lambda event has been started and ended. The REPORT parameter shows metadata such as Duration, Billed duration (Lambda and AWS on the whole takes round values), memory size and max memory used.

<p align="center">
  <img src="https://github.com/CSCI5253-Fall2018/project-3-serverless-anand-ramakrishnan/blob/master/Screenshots/CloudWatch Log Details.PNG" title="CloudWatch Log Details">
</p>
