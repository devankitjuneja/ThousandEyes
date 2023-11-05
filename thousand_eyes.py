import click
from datetime import datetime
from loguru import logger
import dlt
from dlt.sources.helpers import requests

@dlt.source
def thousand_eyes_source(name="thousand_eyes", api_secret_key=dlt.secrets.value, window=None):
    return thousand_eyes_resource(api_secret_key, window)

def _create_auth_headers(api_secret_key):
    headers = {"Authorization": f"Basic {api_secret_key}"}
    return headers

@dlt.resource(name="http-server", write_disposition="merge", merge_key=['date', 'agent_id', 'test_id'])
def thousand_eyes_resource(api_secret_key=dlt.secrets.value, window=None):
    headers = _create_auth_headers(api_secret_key)
    
    tests = get_thousand_eyes_test_data(headers=headers)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for test in tests:
        test_id = test['testId']
        test_name = test['testName']

        http_server = []

        logger.info(f"Making an api call for test_id {test_id} and test_name {test_name}")

        url = f"https://api.thousandeyes.com/v6/web/http-server/{test_id}.json?window={window}"
        while url:
            logger.debug(url)
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            response_json = response.json()

            for row in response_json.get('web', {}).get('httpServer', []):
                row['test_id'] = test_id
                row['test_name'] = test_name
                row['ts_created'] = timestamp
                row['type'] = test['type']

                http_server.append(row)

            response_json['web']['httpServer'] = http_server

            yield response_json['web']['httpServer']

            url = response_json.get('pages', {}).get('next', None)

def get_thousand_eyes_test_data(headers):
    tests = []
    test_types = ['htp-server', 'page-load']
    api_endpoint = "https://api.thousandeyes.com/v6/tests.json"
    logger.debug(f"Getting tests from the api: {api_endpoint}")
    response = requests.get(api_endpoint, headers=headers)
    response.raise_for_status()
    response_json = response.json()

    for json_object in response_json['test']:
        if json_object['type'] in test_types:
            tests.append(json_object)
    
    logger.debug(f"Retreived {len(tests)} tests in {test_types} category")
    return tests

@click.command()
@click.option('--window', help='Window value to use in the API URL')
def main(window):
    if not (window):
        click.echo("Please provide --window option.")
        return

    pipeline = dlt.pipeline(
        pipeline_name='thousand_eyes', destination='mssql', dataset_name='thousand_eyes_data'
    )

    load_info = pipeline.run(thousand_eyes_source(window=window))

    # Pretty print the information on data that was loaded
    print(load_info)

if __name__ == "__main__":
    main()
