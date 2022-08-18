# MIK: code needed to add the header check to the dag.
# Modification needed depending on environment
# Needs to be generalized so it is easier to use
def pyHeaderCheck(**kwargs):

    import os, csv, bz2, pathlib, shutil
    from utils.s3_client import S3Client
    from airflow.hooks.base_hook import BaseHook
    import chardet
    import pandas as pd

    aws_bucket_name = kwargs['aws_bucket_name']
    filepath = kwargs['filepath'][:-1]
    table_config = kwargs['table_config']
    s3_conn = kwargs['s3_conn']
    columns = kwargs['columns']
    filters = kwargs['filters']
    delimiter = filters['delimiter']
    source = kwargs['source']

    connection = BaseHook.get_connection(s3_conn)
    access_key_id = connection.login
    secret_access_key = connection.password

    s3 = S3Client(aws_bucket_name,access_key_id,secret_access_key)

    discovered_files = (s3.search(filepath))
    discovered_csv = [s for s in discovered_files if ".csv" in s]

    # This conditional limits its2 check to the first file in directory
    if source == 'its2':
        discovered_csv = [discovered_csv[0]]
        print(discovered_csv)

    for fileindex, file in enumerate(discovered_csv):

        print("FILE: " + file)

        global data

        filetype = pathlib.Path(file).suffix
        test_file = s3.download_file(file, 'csv_temp' + table_config + str(fileindex) + filetype)
        data = pd.read_csv('csv_temp' + table_config + str(fileindex) + filetype, delimiter=str(delimiter), nrows=10)

        source_header = data.columns

        print('--------------------------------')
        print('| FILEPATH: ' + filepath)
        print('| SOURCE: ' + file)
        print('| SOURCE HEADERS: ' + str(source_header))
        print('| CONFIG HEADERS: ' + str(columns))
        print('| DELIMITER: ' + str(delimiter))
        print('--------------------------------')
        print('| FLAGS ')
        print('| Remove Capitalization: ' +  str(filters['sanitize_capitalization']))
        print('| Remove All Whitespace: ' + str(filters['remove_whitespace']))
        print('| Trim Whitespace: ' + str(filters['trim_whitespace']))
        print('| Ignore Columns: ' + str(filters['ignore_columns']))
        print('| Remove Underscores: ' + str(filters['remove_underscores']))
        print('--------------------------------')


        for index, item in enumerate(columns):
            header_item = source_header[index]
            if(filters['sanitize_capitalization'] == True):
                header_item = header_item.lower()
            if(filters['remove_whitespace'] == True):
                header_item = header_item.strip()
            if(filters['trim_whitespace'] == True):
                header_item = header_item.replace(" ", "")
            if(filters['remove_underscores'] == True):
                header_item = header_item.replace("_", "")
            if(index in filters['ignore_columns']):
                print("| IGNORE  | " + source_header[index])
            elif(str(header_item) == str(item)):
                print("| SUCCESS | " + source_header[index])
            else:
                print("| FAIL    | " + "SOURCE: '" + repr(header_item) + "'" + " | CONFIG: '" + repr(item) + "'")
                print(str(header_item) == str(item))
                raise ValueError('Throwing FAIL, headers do not match.')
                return "This doesn't work"

        os.remove('./csv_temp' + table_config + str(fileindex) + filetype)
