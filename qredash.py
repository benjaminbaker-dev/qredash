import json
import time
import requests

from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option

REDASH_HOST = "http://localhost:5000"
API_KEY = "4fkpGAMpJzWknTpEGXgK7c5OChpbfB2L7KkXtizI"
POLL_INTERVAL = 3


@Configuration()
class QRedash(GeneratingCommand):
    query_id = Option(
        doc='''
        **Syntax:** **query_id=***<123>*
        **Description:** query_id to get results from
        ''',
        require=True
    )

    def generate(self):
        redash_session = type(self)._create_redash_session()
        refresh_data = type(self)._refresh_query(redash_session, self.query_id)

        if refresh_data.status_code != 200:
            raise ValueError("Could not refresh query {}".format(self.query_id))

        refresh_job = refresh_data.json()['job']
        result_id = type(self)._poll_for_new_result(redash_session, refresh_job)

        if not result_id:
            raise ValueError("Query execution failed")

        results_data = type(self)._get_fresh_results(redash_session, self.query_id, result_id)

        if results_data.status_code != 200:
            raise ValueError("Failed fetching result id {}".format(result_id))

        rows = results_data.json()['query_result']['data']['rows']
        for row in rows:
            yield {'_raw': json.dumps(row)}

    @staticmethod
    def _get_fresh_results(session, query_id, result_id):
        response = session.get('{}/api/queries/{}/results/{}.json'.format(REDASH_HOST, query_id, result_id))
        return response

    @staticmethod
    def _poll_for_new_result(session, job):
        while job['status'] not in (3, 4):
            response = session.get('{}/api/jobs/{}'.format(REDASH_HOST, job['id']))
            job = response.json()['job']
            time.sleep(POLL_INTERVAL)

        if job['status'] == 3:
            return job['query_result_id']
        return None

    @staticmethod
    def _refresh_query(session, query_id):
        resp = session.post('{}/api/queries/{}/refresh'.format(REDASH_HOST, query_id))
        return resp

    @staticmethod
    def _create_redash_session():
        session = requests.Session()
        session.headers.update({'Authorization': 'Key {}'.format(API_KEY)})
        return session


dispatch(QRedash)
