# Header Check Example 
# Teddy Caulton and Kevin DuBartell's airflow project

The header check uses the python operator to call py_header_check.py. This python script checks that the headers (column names) for each table in the yaml file match the headers in the S3 bucket. 
If there are any discrenpencies between the two sets of headers, such as different  names or a different order, then the header check throws an error warning that the columns do not match.

The py_header_check.py script collects the header namees of the files stored in the S3 bucket as well as the header names specified in stage_adlogs.yaml. Next, a for loop iterates through each item and index in the list of headers from the yaml. If 'sanitize captilization', 'remove_whitespace', 'trim_whitespace', or 'remove_underscores' are set to true, then those operations are applied to the  header name from S3 with the same index as the current header from the yaml file. By default, these filters are set to false in stage_adlogs.yaml. These filters would be applied before the script checks that the headers match. 

Additionally, if the index of the current header is in the 'ignore columns' list, then that column is ignored and is not checked. The 'ignore columns' list is empty by default and is found in the stage_adlogs.yaml. If the column is not listed to be ignored, then the current item in the loop (the header from yaml) is compared to the header from s3 with the same index. If the columns match, 'success' is printed, otherwise 'failure is printed' and a ValueError is returned. 

In the dag file (stage_adlogs.py), each table is looped through with a for loop. For each table, the py_header_check.py script is called to check the current table's headers. The header check is set downstream of the S3 sensor and upstream of the Snowflake operator. 


