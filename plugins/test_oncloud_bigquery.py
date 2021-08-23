from google.cloud import bigquery
import glob2
import os.path
from string import Template

# Parameterize test so that it runs for each '_test.sql' file in this directory
def pytest_generate_tests(metafunc):
    filelist = glob2.iglob('./**/*_test.sql', recursive=True)
    metafunc.parametrize("sql_test_file_name", filelist)

def test_changes(sql_test_file_name):
    test_file_dir = os.path.dirname(sql_test_file_name)
    with open(sql_test_file_name) as sql_test_file:

        # Replace templates $changes, $deployments, $incidents with the real sql.
        sql_str = sql_test_file.read()
        for template in ['changes', 'deployments', 'incidents']:
            template_path = os.path.join(test_file_dir, template+'.sql')
            if os.path.isfile(template_path):
                with open(template_path) as template_file:
                    # It is necessary to replace the source table in the real sql with the temporary test data reference.
                    sql_str = Template(sql_str).safe_substitute({template: template_file.read().replace('four_keys.events_raw', 'events_raw')})

        client = bigquery.Client()


        query_job = client.query(sql_str)

        for row in query_job:
            print(row)