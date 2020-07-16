CREATE DATABASE Transaction_DB;

USE DATABASE Transaction_DB;

CREATE SCHEMA file_formats;

CREATE SCHEMA external_stages;

CREATE SCHEMA snowpipes;

-- AWS S3 Configuration (ACCOUNTADMIN has the privilege)
CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::111222333444:role/snowflake_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-private/knoldus/load_data/');

-- create a file format describing the continuous data files
CREATE OR REPLACE FILE FORMAT file_formats.snowpipe_csv_format
TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 1 NULL_IF = ('NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE;

-- create an external stage using an S3 bucket
CREATE OR REPLACE STAGE external_stages.transaction_events STORAGE_INTEGRATION = s3_int
URL ='s3://snowflake-private/knoldus/load_data/' 
FILE_FORMAT = file_formats.snowpipe_csv_format;

-- list the files already present in the bucket
LIST @external_stages.transaction_events;

CREATE TRANSIENT TABLE public.transactionDetails (       
   Transaction_Date DATE,
   Customer_ID NUMBER,
   Transaction_ID NUMBER,
   Amount NUMBER
);

DESC TABLE public.transactionDetails;

-- test COPY command
COPY INTO public.transactionDetails FROM @external_stages.transaction_events FILE_FORMAT = file_formats.snowpipe_csv_format
ON_ERROR = 'CONTINUE';

-- check for data in the table
SELECT COUNT(*) FROM public.transactionDetails;

TRUNCATE TABLE public.transactionDetails;

-- create the pipe
CREATE OR REPLACE PIPE snowpipes.transaction_pipe
AUTO_INGEST = true
AS
COPY INTO public.transactionDetails FROM @external_stages.transaction_events
FILE_FORMAT = file_formats.snowpipe_csv_format
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) FROM public.transactionDetails;

SHOW PIPES;

--After loading new data to S3

SELECT COUNT(*) FROM public.transactionDetails;

TRUNCATE TABLE public.transactionDetails;

