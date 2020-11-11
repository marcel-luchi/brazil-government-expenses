import os
import json
import requests
import boto3
from requests import RequestException
from airflow.models import BaseOperator
import time
from airflow.contrib.hooks.aws_hook import AwsHook


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
                 base_url,
                 aws_conn_id,
                 s3_bucket,
                 filename,
                 api_endpoint,
                 access_key,
                 pagina=1,
                 num_retries=5,
                 sleep_time=0,
                 dependency_bucket="",
                 dependency_file="",
                 dependency_params=[],
                 limit_pages=0,
                 *args, **kwargs
                 ):

        super(TransparenciaApiReaderOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.base_url = base_url
        self.s3_bucket = s3_bucket
        self.filename = filename
        self.pagina = pagina
        self.num_retries = num_retries
        self.sleep_time = sleep_time
        self.api_endpoint = api_endpoint
        self.url = base_url + api_endpoint
        self.headers = {"Accept": "*/*",
                        "chave-api-dados": access_key}
        self.dependency_bucket = dependency_bucket
        self.dependency_file = dependency_file
        self.dependency_params = dependency_params
        self.month = ''
        self.year = ''
        self.limit_pages = limit_pages

    def __param_generator(self):
        """Generates a list consisting in dictionaries that will be used in request for endpoints dependent
        of values retrieved in another endpoint."""
        self.log.info("Reading dependent parameters.")
        with open(os.path.join(self.dependency_bucket.format(month=self.month, year=self.year),
                               self.dependency_file.format(month=self.month, year=self.year)), "r") as f:
            data = json.loads(f.read())

        return list(map(lambda item: dict(map(lambda key: (key[1], item.get(key[0])),
                                              self.dependency_params)),
                        data))

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
        return json.dumps(data, ensure_ascii=False)

    def __process_vouchers(self):
        """Method used to Retrieve Brazil's Federal Government
           Corporate Credit Card Vouchers from Portal da Transparencia
           As Year/Month (mesExtratoInicio/mesExtratoFim) are obrigatory request params
           They are retrieved from Airflow Execution Date."""

        parameters = {"mesExtratoInicio": f"{self.month}/{self.year}",
                      "mesExtratoFim": f"{self.month}/{self.year}",
                      "pagina": self.pagina}
        return self.__fetch_pages(parameters)

    def execute(self, context):
        """Method called by Airflow Task."""
        self.month = "{execution_date.month}".format(**context).zfill(2)
        self.year = "{execution_date.year}".format(**context)
        aws = AwsHook(self.aws_conn_id)
        credentials = aws.get_credentials()
        s3 = boto3.resource('s3',
                            region_name='us-west-2',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key
                            )

        filename = self.filename.format(month=self.month, year=self.year)
        s3.Object(self.s3_bucket, filename).put(Body=self.__process_vouchers())
