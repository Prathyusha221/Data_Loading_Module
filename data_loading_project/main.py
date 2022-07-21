import sys
from configparser import ConfigParser
from sqlalchemy import create_engine
import boto3
import pysftp
import datetime
from stat import S_ISDIR
import fnmatch
import pandas as pd
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


# Reading config file
config_file = sys.argv[1]
config = ConfigParser()
config.read(config_file)

sender_email = config.get('email_credentials', 'email_user')
mail_password = config.get('email_credentials', 'email_password')
mail_domain = sender_email.split("@")[-1]
port = config.get('email_credentials', 'mail_server_port')  # For starttls
smtp_server = config.get('email_credentials', mail_domain)

print(port, smtp_server)


def get_column_dtypes(datatypes):
    data_list = []
    for x in datatypes:
        if x == 'int64' or x == 'integer':
            data_list.append('int')
        elif x == 'float64' or x == 'double precision':
            data_list.append('float')
        elif x == 'bool' or x == 'boolean':
            data_list.append('boolean')
        elif x == 'datetime64' or x == 'timestamp without time zone':
            data_list.append('timestamp')
        elif x == 'timedelta' or x == 'interval':
            data_list.append('interval')
        else:
            data_list.append('varchar(255)')
    return data_list


if __name__ == '__main__':
    file_id = str(sys.argv[-1])

    # Connection to database
    try:
        conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
        conn.execution_options(autocommit=True)
    except Exception as ex:
        print("Connection unsuccessful: ", ex)
        exit(1)

    # Fetching file details based on the given file_id
    details = conn.execute(f"""SELECT * FROM {config.get('database', 'transaction_table_schema')}.{config.get('database', 'transaction_table')} WHERE file_id = '{file_id}'""")

    file_details_row = details.fetchone()
    # If nothing is fetched
    if file_details_row is None:
        print("The details of the file with file_id {} doesn't exist".format(file_id))
        exit(1)

    email_id = file_details_row['owner_email']
    storage_type = file_details_row['storage_type']
    source = file_details_row['source_location']
    file_pattern = file_details_row['file_name_pattern']

    # set default target schema and table name if not given
    target_schema = file_details_row['target_schema']
    if target_schema is None:
        target_schema = config.get('database', 'transaction_table_schema')
    target_table = file_details_row['target_table']
    if target_table is None:
        target_table = 'tmp_' + file_id

    receiver_email = [email_id, sender_email]  # a list of recipients

    filename = []
    df_list = []
    df_dict = {}
    picked_at_dict = {}
    local_file_dict = {}

    # For files with storage type S3
    if storage_type == 'S3':
        s3_bucket_name = file_details_row['host_or_bucket']
        s3_access_key_id = file_details_row['username']
        s3_secret_access_key = file_details_row['password']
        archive_directory = config.get('database', 's3_bucket_archive_directory')
        key = source

        # Connection to S3 bucket
        try:
            s3_client = boto3.client('s3', aws_access_key_id=s3_access_key_id,
                                     aws_secret_access_key=s3_secret_access_key)
        except Exception as ex:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg['To'] = email_id
            msg['CC'] = sender_email

            msg["Subject"] = 'Error occurred'
            # Plain text
            text = """\
                Error occurred while connecting to the S3 bucket.
                Kindly check the login details provided.
                """

            body_text = MIMEText(text, 'plain')
            msg.attach(body_text)  # attaching the text body into msg

            try:
                # Create your SMTP session
                smtp = smtplib.SMTP(smtp_server, port)

                # Use TLS to add security
                smtp.starttls()

                # User Authentication
                smtp.login(sender_email, mail_password)

                # Sending the Email
                smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                # Terminating the session
                smtp.quit()
                print("Email sent successfully!")

            except Exception as ex:
                print("Something went wrong....", ex)
            conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
            conn.execution_options(autocommit=True)
            status = 'fail'
            conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, source_location, status, target_schema, target_table)
                         VALUES('{file_id}', '{source}', '{status}', '{target_schema}', '{target_table}')""")
            exit(1)

        # Filtering out the required files
        try:
            for my_bucket_object in s3_client.list_objects(Bucket=s3_bucket_name, Prefix=key)['Contents']:
                if fnmatch.fnmatch(my_bucket_object['Key'], key + file_pattern):
                    filename.append(my_bucket_object['Key'])
                    picked_at_dict[my_bucket_object['Key']] = datetime.datetime.now()

            print(filename)
            if filename:
                pass
            else:
                raise Exception
        except Exception as ex:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg['To'] = email_id
            msg['CC'] = sender_email

            msg["Subject"] = 'Error occurred'
            # Plain text
            text = """\
                No file matches the given file pattern %s at the location %s
                """ % (file_pattern, source)

            body_text = MIMEText(text, 'plain')
            msg.attach(body_text)  # attaching the text body into msg

            try:
                # Create your SMTP session
                smtp = smtplib.SMTP(smtp_server, port)

                # Use TLS to add security
                smtp.starttls()

                # User Authentication
                smtp.login(sender_email, mail_password)

                # Sending the Email
                smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                # Terminating the session
                smtp.quit()
                print("Email sent successfully!")

            except Exception as ex:
                print("Something went wrong....", ex)
            conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
            conn.execution_options(autocommit=True)
            status = 'fail'
            conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, source_location, status, target_schema, target_table)
                         VALUES('{file_id}', '{source}', '{status}', '{target_schema}', '{target_table}')""")
            exit(1)

        # Download and read the files
        for file in filename:
            try:
                local_file = config.get('database', 'local_directory') + '/' + file.replace(key, "")
                s3_client.download_file(s3_bucket_name, file, local_file)
                local_file_dict[file] = local_file
                filetype = (local_file.split('.'))[-1]
                if filetype == 'csv':
                    data = pd.read_csv(local_file)
                elif filetype == 'xlsx':
                    data = pd.read_excel(local_file)
                else:
                    raise Exception
                df = pd.DataFrame(data)
                df_list.append(df)
                df_dict[file] = df
                print(df_list)

            except Exception as ex:
                os.remove(local_file)
                msg = MIMEMultipart()
                msg["From"] = sender_email
                msg['To'] = email_id
                msg['CC'] = sender_email

                msg["Subject"] = 'Error occurred'
                # Plain text
                text = """\
                Couldn't parse the file %s with file_id %s at the location 
                %s
                Check the file format
                """ % (file.replace(key, ""), file_id, source)

                body_text = MIMEText(text, 'plain')
                msg.attach(body_text)  # attaching the text body into msg

                try:
                    # Create your SMTP session
                    smtp = smtplib.SMTP(smtp_server, port)

                    # Use TLS to add security
                    smtp.starttls()

                    # User Authentication
                    smtp.login(sender_email, mail_password)

                    # Sending the Email
                    smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                    # Terminating the session
                    smtp.quit()
                    print("Email sent successfully!")

                except Exception as ex:
                    print("Something went wrong....", ex)
                conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
                conn.execution_options(autocommit=True)
                status = 'fail'
                conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, filename, source_location, picked_at, status, target_schema, target_table)
                              VALUES('{file_id}', '{file.replace(source, "")}', '{source}', '{picked_at_dict[file]}', '{status}', '{target_schema}', '{target_table}')""")

        print(df_dict)

    # For files with storage type as SFTP
    elif storage_type == 'SFTP':
        file_host = file_details_row['host_or_bucket']
        file_username = file_details_row['username']
        file_password = file_details_row['password']
        source_name = source
        archive_path = source_name[:-1] + '_archive/'
        print(archive_path)
        archive_path_parts = archive_path[:-1].split("/")
        print(archive_path_parts)
        main_directory = archive_path.replace(archive_path_parts[-1] + '/', "")

        # Connection to server location
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            sftp_client = pysftp.Connection(host=file_host, username=file_username, password=file_password,
                                            cnopts=cnopts)
            # Switch to a remote directory
            sftp_client.cwd(source_name)
            # Obtain structure of the remote directory '/var/www/vhosts'
            directory_structure = sftp_client.listdir()  # _attr()
            print(directory_structure)
            print(main_directory)

            archiveExists = 0
            print(archiveExists)

            # check if archive directory exists
            # if not, create the directory
            for attr in sftp_client.listdir_attr(main_directory):
                mode = attr.st_mode
                if S_ISDIR(mode) and attr.filename == archive_path_parts[-1]:
                    archiveExists = 1

            print(archiveExists)
            if archiveExists == 0:
                sftp_client.mkdir(archive_path)
        except Exception as ex:
            print(ex)
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg['To'] = email_id
            msg['CC'] = sender_email

            msg["Subject"] = 'Error occurred'
            # Plain text
            text = """\
            Error occurred while connecting to the server.
            Kindly check the server details provided.
            """

            body_text = MIMEText(text, 'plain')
            msg.attach(body_text)  # attaching the text body into msg

            try:
                # Create your SMTP session
                smtp = smtplib.SMTP(smtp_server, port)

                # Use TLS to add security
                smtp.starttls()

                # User Authentication
                smtp.login(sender_email, mail_password)

                # Sending the Email
                smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                # Terminating the session
                smtp.quit()
                print("Email sent successfully!")

            except Exception as ex:
                print("Something went wrong....", ex)
            conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
            conn.execution_options(autocommit=True)
            status = 'fail'
            conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, source_location, status, target_schema, target_table)
                         VALUES('{file_id}', '{source}', '{status}', '{target_schema}', '{target_table}')""")
            exit(1)

        # Filtering out the files
        try:
            for attr in directory_structure:
                if fnmatch.fnmatch(attr, file_pattern):
                    filename.append(attr)
                    picked_at_dict[attr] = datetime.datetime.now()
            print(filename)
            if filename:
                pass
            else:
                raise Exception

        except Exception as ex:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg['To'] = email_id
            msg['CC'] = sender_email

            msg["Subject"] = 'Error occurred'
            # Plain text
            text = """\
                   No file matches the given file pattern %s at the location %s
                   """ % (file_pattern, source)

            body_text = MIMEText(text, 'plain')
            msg.attach(body_text)  # attaching the text body into msg

            try:
                # Create your SMTP session
                smtp = smtplib.SMTP(smtp_server, port)

                # Use TLS to add security
                smtp.starttls()

                # User Authentication
                smtp.login(sender_email, mail_password)

                # Sending the Email
                smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                # Terminating the session
                smtp.quit()
                print("Email sent successfully!")

            except Exception as ex:
                print("Something went wrong....", ex)
            conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
            conn.execution_options(autocommit=True)
            status = 'fail'
            conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, source_location, status, target_schema, target_table)
                          VALUES('{file_id}', '{source}', '{status}', '{target_schema}', '{target_table}')""")
            exit(1)

        # Download and read the required files
        for file in filename:
            try:
                local_file = config.get('database', 'local_directory') + '/' + file
                sftp_client.get(source_name + file, local_file)
                local_file_dict[file] = local_file
                filetype = (local_file.split('.'))[-1]
                if filetype == 'csv':
                    data = pd.read_csv(local_file)
                elif filetype == 'xlsx':
                    data = pd.read_excel(local_file)
                else:
                    raise Exception
                df = pd.DataFrame(data)
                df_list.append(df)
                print(df_list)
                df_dict[file] = df

            except Exception as ex:
                os.remove(local_file)
                msg = MIMEMultipart()
                msg["From"] = sender_email
                msg['To'] = email_id
                msg['CC'] = sender_email

                msg["Subject"] = 'Error occurred'
                # Plain text
                text = """\
                Couldn't parse the file %s with file_id %s at the location 
                %s
                Check the file format
                """ % (file, file_id, source)

                body_text = MIMEText(text, 'plain')
                msg.attach(body_text)  # attaching the text body into msg

                try:
                    # Create your SMTP session
                    smtp = smtplib.SMTP(smtp_server, port)

                    # Use TLS to add security
                    smtp.starttls()

                    # User Authentication
                    smtp.login(sender_email, mail_password)

                    # Sending the Email
                    smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                    # Terminating the session
                    smtp.quit()
                    print("Email sent successfully!")

                except Exception as ex:
                    print("Something went wrong....", ex)
                conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
                conn.execution_options(autocommit=True)
                status = 'fail'
                conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, filename, source_location, picked_at, status, target_schema, target_table)
                                             VALUES('{file_id}', '{file}', '{source}', '{picked_at_dict[file]}', '{status}', '{target_schema}', '{target_table}')""")
        sftp_client.close()
        print(df_dict)
    # If storage type is other than S3 or server
    else:
        print("\nInappropriate storage_type")
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg['To'] = email_id
        msg['CC'] = sender_email

        msg["Subject"] = 'Error occurred'
        # Plain text
        text = """\
        Inappropriate storage_type
        Supported types: SFTP, S3
        """

        body_text = MIMEText(text, 'plain')
        msg.attach(body_text)  # attaching the text body into msg

        try:
            # Create your SMTP session
            smtp = smtplib.SMTP(smtp_server, port)

            # Use TLS to add security
            smtp.starttls()

            # User Authentication
            smtp.login(sender_email, mail_password)

            # Sending the Email
            smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

            # Terminating the session
            smtp.quit()
            print("Email sent successfully!")

        except Exception as ex:
            print("Something went wrong....", ex)
        conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
        conn.execution_options(autocommit=True)
        status = 'fail'
        conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, source_location, status, target_schema, target_table)
                                                     VALUES('{file_id}', '{source}', '{status}', '{target_schema}', '{target_table}')""")

    # Transferring dataframe(s) to target table
    for file in df_dict.keys():
        if storage_type == 'S3':
            file_name = file.replace(source, "")
        else:
            file_name = file
        before_processing_rows = df_dict[file].shape[0]

        column_name = list(df_dict[file].columns.values)
        column_data_type = get_column_dtypes(df_dict[file].dtypes)
        df_column_dict = dict(zip(column_name, column_data_type))

        create_table_statement = 'CREATE TABLE IF NOT EXISTS ' + target_schema + '.' + target_table + ' ('

        for i in range(len(column_data_type)):
            create_table_statement = create_table_statement + '\n' + column_name[i] + ' ' + column_data_type[i] + ','

        create_table_statement = create_table_statement[:-1] + ' );'

        try:
            conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
            conn.execution_options(autocommit=True)
            conn.execute(create_table_statement)

            # Fetch target table columns and their datatypes if exist already
            table_details = conn.execute(f"""SELECT COLUMN_NAME,data_type FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{target_table}' AND TABLE_SCHEMA='{target_schema}'""")

            table_column_name = []
            table_column_data_type = []
            for row in table_details:
                table_column_name.append(row[0])
                table_column_data_type.append(row[1])
            table_column_data_type = get_column_dtypes(table_column_data_type)
            table_column_dict = dict(zip(table_column_name, table_column_data_type))

            initial_count_query = 'select count(*) from ' + target_schema + '.' + target_table
            initial_count = conn.execute(initial_count_query)
            for r in initial_count:
                for c in r:
                    initial_rows = c
        except Exception as ex:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg['To'] = email_id
            msg['CC'] = sender_email

            msg["Subject"] = 'Error occurred'
            # Plain text
            text = """\
                Database connection failed while transferring data of file %s with file_id %s""" % (file_name, file_id)

            body_text = MIMEText(text, 'plain')
            msg.attach(body_text)  # attaching the text body into msg
            status = 'fail'

            try:
                # Create your SMTP session
                smtp = smtplib.SMTP(smtp_server, port)

                # Use TLS to add security
                smtp.starttls()

                # User Authentication
                smtp.login(sender_email, mail_password)

                # Sending the Email
                smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                # Terminating the session
                smtp.quit()
                print("Email sent successfully!")

            except Exception as ex:
                print("Something went wrong....", ex)
            conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
            conn.execution_options(autocommit=True)
            conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, filename, source_location, picked_at, status, before_processing_rows, target_schema, target_table)
                        VALUES('{file_id}', '{file_name}', '{source}', '{picked_at_dict[file]}', '{status}', '{before_processing_rows}', '{target_schema}', '{target_table}')""")
            continue

        # Check if the datatypes matches in both target table and dataframe
        # Also check for any extra columns
        want_to_break = False
        if table_column_name is not None:
            for col in column_name:
                if col in table_column_name:
                    try:
                        if df_column_dict[col] == table_column_dict[col]:
                            pass
                        else:
                            raise Exception
                    except Exception as ex:
                        msg = MIMEMultipart()
                        msg["From"] = sender_email
                        msg['To'] = email_id
                        msg['CC'] = sender_email

                        msg["Subject"] = 'Error occurred'
                        # Plain text
                        text = """\
                            Table Columns are of different data types.
                            Recheck the file %s with file_id %s
                                    """ % (file_name, file_id)

                        body_text = MIMEText(text, 'plain')
                        msg.attach(body_text)  # attaching the text body into msg
                        status = 'fail'

                        try:
                            # Create your SMTP session
                            smtp = smtplib.SMTP(smtp_server, port)

                            # Use TLS to add security
                            smtp.starttls()

                            # User Authentication
                            smtp.login(sender_email, mail_password)

                            # Sending the Email
                            smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                            # Terminating the session
                            smtp.quit()
                            print("Email sent successfully!")

                        except Exception as ex:
                            print("Something went wrong....", ex)
                        conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, filename, source_location, picked_at, status, before_processing_rows, target_schema, target_table)
                                                                     VALUES('{file_id}', '{file_name}', '{source}', '{picked_at_dict[file]}', '{status}', '{before_processing_rows}', '{target_schema}', '{target_table}')
                                                                     """)
                        want_to_break = True
                        break
                else:
                    try:
                        conn.execute(f"""ALTER TABLE {target_schema}.{target_table}
                            ADD {col} {df_column_dict[col]}""")
                    except Exception as ex:
                        print(ex)
                        msg = MIMEMultipart()
                        msg["From"] = sender_email
                        msg['To'] = email_id
                        msg['CC'] = sender_email

                        msg["Subject"] = 'Error occurred'
                        # Plain text
                        text = """\
                            Failure in creating table column for file %s with file_id %s""" % (file_name, file_id)

                        body_text = MIMEText(text, 'plain')
                        msg.attach(body_text)  # attaching the text body into msg
                        status = 'fail'

                        try:
                            # Create your SMTP session
                            smtp = smtplib.SMTP(smtp_server, port)

                            # Use TLS to add security
                            smtp.starttls()

                            # User Authentication
                            smtp.login(sender_email, mail_password)

                            # Sending the Email
                            smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                            # Terminating the session
                            smtp.quit()
                            print("Email sent successfully!")

                        except Exception as ex:
                            print("Something went wrong....", ex)
                        conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, filename, source_location, picked_at, status, before_processing_rows, target_schema, target_table)
                                    VALUES('{file_id}', '{file_name}', '{source}', '{picked_at_dict[file]}', '{status}', '{before_processing_rows}', '{target_schema}', '{target_table}')""")
                        want_to_break = True
                        break
        if want_to_break:
            break
        else:
            pass

        # dataframe to target table
        try:
            conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
            conn.execution_options(autocommit=True)
            df_dict[file].to_sql(con=conn, name=target_table, schema=target_schema, if_exists='append', index=False)
        except Exception as ex:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg['To'] = email_id
            msg['CC'] = sender_email

            msg["Subject"] = 'Error occurred'
            # Plain text
            text = """\
                Couldn't transfer the file %s with file_id %s to
                Target schema: %s
                Target table: %s

                """ % (file_name, file_id, target_schema, target_table)

            body_text = MIMEText(text, 'plain')
            msg.attach(body_text)  # attaching the text body into msg
            status = 'fail'

            try:
                # Create your SMTP session
                smtp = smtplib.SMTP(smtp_server, port)

                # Use TLS to add security
                smtp.starttls()

                # User Authentication
                smtp.login(sender_email, mail_password)

                # Sending the Email
                smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                # Terminating the session
                smtp.quit()
                print("Email sent successfully!")

            except Exception as ex:
                print("Something went wrong....", ex)
            conn = create_engine(f"postgresql+psycopg2://{config.get('database', 'username')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'database')}")
            conn.execution_options(autocommit=True)
            conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, filename, source_location, picked_at, status, before_processing_rows, target_schema, target_table)
                             VALUES('{file_id}', '{file_name}', '{source}', '{picked_at_dict[file]}', '{status}', '{before_processing_rows}', '{target_schema}', '{target_table}')""")
            continue

        # Moving files to archive location and delete from local directory
        try:
            if storage_type == 'S3':
                archive_file_name = config.get('database', 's3_bucket_archive_directory') + "/" + file.replace(source, "")
                fp = open(local_file_dict[file], 'rb')
                result = s3_client.put_object(Body=fp, Bucket=file_details_row['host_or_bucket'], Key=archive_file_name)
                fp.close()
                res = result.get('ResponseMetadata')

                if res.get('HTTPStatusCode') == 200:
                    print('File Uploaded Successfully')
                else:
                    print('File Not Uploaded')

                os.remove(local_file_dict[file])
            # delete file from S3 bucket

            #    s3_client = boto3.client('s3', aws_access_key_id=file_details_row['username'],
            #                                   aws_secret_access_key=file_details_row['password'])
            #    result2 = s3_client.delete_object(Bucket=file_details_row['host_or_bucket'], Key=file)
            #    res = result2.get('ResponseMetadata')
            #
            #    if res.get('Delete Marker'):
            #        print('deleted')
            #    else:
            #        print('File still exists')
            #        raise Exception
            elif storage_type == 'SFTP':
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None
                sftp_client = pysftp.Connection(host=file_details_row['host_or_bucket'], username=file_details_row['username'], password=file_details_row['password'], cnopts=cnopts)
                sftp_client.rename(source + file, source[:-1] + '_archive/' + file)
                print("file moved to archive location")
                os.remove(local_file_dict[file])
                print("file deleted from local directory")
            else:
                print("file is not moved to archive location")
                raise Exception
        except Exception as ex:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg['To'] = email_id
            msg['CC'] = sender_email

            msg["Subject"] = ''
            # Plain text
            text = """\
            File %s with file_id %s is not moved to the archive location
            Or deleted from the local directory""" % (file_name, file_id)

            body_text = MIMEText(text, 'plain')
            msg.attach(body_text)  # attaching the text body into msg
            status = 'fail'

            try:
                # Create your SMTP session
                smtp = smtplib.SMTP(smtp_server, port)

                # Use TLS to add security
                smtp.starttls()

                # User Authentication
                smtp.login(sender_email, mail_password)

                # Sending the Email
                smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

                # Terminating the session
                smtp.quit()
                print("Email sent successfully!")

            except Exception as ex:
                print("Something went wrong....", ex)

        count_query = 'select count(*) from ' + target_schema + '.' + target_table
        processed = conn.execute(count_query)
        for r in processed:
            for c in r:
                processed_rows = c

        updated_rows = (processed_rows - initial_rows)

        if before_processing_rows == updated_rows:
            status = 'success'
        else:
            status = 'error occurred'

        picked_at_secs = picked_at_dict[file].timestamp()
        inserted_at = datetime.datetime.now()
        inserted_at_secs = inserted_at.timestamp()
        time_taken = inserted_at_secs - picked_at_secs
        time_taken = round(time_taken, 2)

        # Audit the transaction
        conn.execute(f"""INSERT INTO {config.get('database', 'audit_table_schema')}.{config.get('database', 'audit_table')}(file_id, filename, source_location, picked_at, status, before_processing_rows, processed_rows, target_schema, target_table)
                         VALUES('{file_id}', '{file_name}', '{source}', '{picked_at_dict[file]}', '{status}', '{before_processing_rows}', '{updated_rows}', '{target_schema}', '{target_table}')""")

        print("Auditing successful")
        print("Sending detailed mail to the owner")

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg['To'] = email_id
        msg['CC'] = sender_email

        if status == 'success':
            msg["Subject"] = 'File transfer has been completed'
            # Plain text
            text = """\
                Source: %s
                File: %s
                Target: %s.%s
                Total records processed: %s
                Total time taken: %s seconds
                """ % (source, file_name, target_schema, target_table, updated_rows, time_taken)
        else:
            msg["Subject"] = 'Error occurred during file transfer'
            # Plain text
            text = """\
                Source: %s
                File: %s
                Target: %s.%s
                Total records processed: %s
                Total time taken: %s seconds
                """ % (source, file_name, target_schema, target_table, updated_rows, time_taken)

        body_text = MIMEText(text, 'plain')
        msg.attach(body_text)  # attaching the text body into msg

        try:
            # Create your SMTP session
            smtp = smtplib.SMTP(smtp_server, port)

            # Use TLS to add security
            smtp.starttls()

            # User Authentication
            smtp.login(sender_email, mail_password)

            # Sending the Email
            smtp.sendmail(sender_email, ",".join(receiver_email), msg.as_string())

            # Terminating the session
            smtp.quit()
            print("Email sent successfully!")
        except Exception as ex:
            print("Something went wrong....", ex)
    print("Program ends here")
