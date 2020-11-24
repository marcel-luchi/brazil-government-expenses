import os
import re
import json
import requests
import boto3
from requests import RequestException
from airflow.models import BaseOperator
import time
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers.constants import RAW_DIR, VOUCHERS_DIR


class TransparenciaApiReaderOperator(BaseOperator):
    """This class will consume data from Brazil's Portal da Transparencia API,
       in which data from federal government is made public.

       Some API endpoints use parameters extracted from other endpoints
       This is not in use, but is already implemented,
       as only Corporate Card Vouchers are implemented.

       Dependency will read a S3 object, retrieving the columns in dependency_params
       arg and generate a list of dicts for each distinct value in which
       the keys are the name of the parameters
       and the value is the data found in that column.

       Args:
           base_url (str): URL for portal da transparencia API
           aws_conn_id (str): Airflow AWS connection, needed to conect to bucket
           s3_bucket (str): S3 bucket name in which data will be saved
           filename (str) Filename to be saved in s3 bucket
           api_endpoint (str): Portal da Transparencia API Endpoint
           access_key(str): Portal da Transparencia Access Key needed to consume data
           pagina (int): Page to start consuming data (usually 1)
           num_retries (int): Number of times a request will be retried to server in case of errors
           sleep_time (int): Sleep time between requests, usually 0, increase if many requests fail
           dependency_bucket (str): S3 file that needs to be consumed for API endpoints
                                    that need keys from other extractions
           dependency_params (list of dict): Columns that need to be extracted from a dependent file,
                                             format [{colum_name: request_param_name}]
           limit_pages (int): Limit number of pages retrieved, used for testing, as a full extraction,
                              may take up to one hour.

    """

    def __init__(self,
                 aws_conn_id,
                 storage_type,
                 extraction_type,
                 s3_bucket,
                 access_key,
                 num_retries=5,
                 sleep_time=1,
                 limit_pages=0,
                 *args, **kwargs
                 ):

        super(TransparenciaApiReaderOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = re.sub('^.*//', '', s3_bucket)
        self.page = 1
        self.num_retries = num_retries
        self.sleep_time = sleep_time
        self.extraction_type = extraction_type
        self.headers = {"Accept": "*/*",
                        "chave-api-dados": access_key}
        self.url = ''
        self.limit_pages = limit_pages
        self.storage_type = storage_type
        self.year = ''
        self.month = ''
        self.day = ''
        self.extractions = {}

    def __process(self, filename, processing_method, context):
        if self.storage_type == "local":
            with open(os.path.join(self.s3_bucket, RAW_DIR, VOUCHERS_DIR, filename).format(**context), 'w')  as f:
                f.write(json.dumps(processing_method(context), ensure_ascii=False))
        else:
            aws = AwsHook(self.aws_conn_id)
            credentials = aws.get_credentials()
            s3 = boto3.resource('s3',
                                region_name='us-west-2',
                                aws_access_key_id=credentials.access_key,
                                aws_secret_access_key=credentials.secret_key
                                )
            s3.Object(self.s3_bucket.format(**context), os.path.join(RAW_DIR,
                                                                     VOUCHERS_DIR,
                                                                     filename.format(**context)))\
                .put(Body=json.dumps(processing_method(context), ensure_ascii=False))

    def __read_file(self, filename):
        if self.storage_type == "local":
            with open(os.path.join(self.s3_bucket, filename), 'r') as f:
                return json.loads(f.read())
        else:
            aws = AwsHook(self.aws_conn_id)
            credentials = aws.get_credentials()
            s3 = boto3.resource('s3',
                                region_name='us-west-2',
                                aws_access_key_id=credentials.access_key,
                                aws_secret_access_key=credentials.secret_key
                                )
            return json.loads(s3.Object(self.s3_bucket, filename).get['Body'].read())

    def __fetch_page(self, params, num_retries, sleep_time):
        """Fetch one JSON page from Portal da Transparencia API"""
        retries = 0
        while True:
            if retries > num_retries:
                raise RequestException
            # due to limitations in API that limits requests to 90 in a minute
            time.sleep(sleep_time)
            try:
                resp = requests.get(self.url, params=params, headers=self.headers)
                if resp.status_code != 200:
                    self.log.info("{} {} {}".format(self.url, params, self.headers))
                    self.log.info(resp.status_code)
                    self.log.info(resp.content)
                    retries += 1
                    continue
                self.log.info("Fetched page {}, content size: {}".format(params.get("pagina"), len(resp.content)))
                return resp.json()
            except RequestException:
                self.log.info("Request failed.")
                self.log.info("{} {} {}".format(self.url, params, self.headers))
                retries += 1
                continue

    def __fetch_pages(self, parameters):
        """Retrieves pages from Portal da Transparencia API until no content is available or limit_pages is reached"""
        data = []
        while True:
            curr_page_vouchers = self.__fetch_page(parameters, self.num_retries, self.sleep_time)
            if len(curr_page_vouchers) == 0:
                break
            data.extend(curr_page_vouchers)
            curr_page = parameters.get("pagina")
            if curr_page is None or (parameters.get("pagina") >= self.limit_pages > 0):
                break
            parameters.update({"pagina": curr_page + 1})
        return data

    def __fetch_default(self, context):
        parameters = {"pagina": self.page}

        return self.__fetch_pages(parameters)

    def __fetch_agency_expenses(self, context):
        agency_params = [('codigo', 'unidadeGestora')]
        agency_expenses = []
        params_list = list(map(lambda item: dict(map(lambda param: (param[1], item.get(param[0])),
                                                     agency_params)),
                               self.__read_file(self.extractions.get('government_agencies')
                                                .get('filename')
                                                .format(**context))))
        for params in params_list:
            for expense_phase in [1, 2, 3]:
                params.update({'pagina': 1,
                               'dataEmissao': f'{self.day}/{self.month}/{self.year}',
                               'fase': expense_phase})
                self.log.info(f"Fetching expenses for agency_code {params.get('unidadeGestora')} and phase {expense_phase}\n")
                agency_expenses.extend(self.__fetch_pages(params))
        return agency_expenses

    def __fetch_corporate_card_expenses(self, context):
        """Method used to Retrieve Brazil's Federal Government
           Corporate Credit Card Vouchers from Portal da Transparencia
           As Year/Month (mesExtratoInicio/mesExtratoFim) are obrigatory request params
           They are retrieved from Airflow Execution Date."""

        parameters = {"mesExtratoInicio": f"{self.month}/{self.year}",
                      "mesExtratoFim": f"{self.month}/{self.year}",
                      "pagina": self.page}
        return self.__fetch_pages(parameters)

    def execute(self, context):
        """Method called by Airflow Task."""
        base_url = 'http://transparencia.gov.br/api-de-dados'
        self.year = '{execution_date.year}'.format(**context)
        self.month = '{execution_date.month}'.format(**context).zfill(2)
        self.day = '{execution_date.day}'.format(**context).zfill(2)

        self.extractions = {'corporate_card_expenses': {'endpoint': '/cartoes',
                                                        'filename': 'corporate-card-expenses-{ds}.json',
                                                        'method': self.__fetch_corporate_card_expenses},
                            'government_agencies': {'endpoint': '/orgaos-siafi',
                                                    'filename': 'government-agency-{ds}.json',
                                                    'method': self.__fetch_default},
                            'agency_expenses_documents': {'endpoint': '/despesas/documentos',
                                                          'filename': 'agency-expenses-{ds}.json',
                                                          'method': self.__fetch_agency_expenses}}
        if self.extractions.get(self.extraction_type) is None:
            self.log.info(f"Extraction method is not valid, possible values are: {self.extractions.keys ()}")
            raise Exception

        self.url = base_url + self.extractions.get(self.extraction_type).get('endpoint')
        self.__process(self.extractions.get(self.extraction_type).get('filename'),
                       self.extractions.get(self.extraction_type).get('method'),
                       context)
