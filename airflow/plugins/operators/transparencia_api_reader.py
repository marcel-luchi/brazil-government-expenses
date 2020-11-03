import os
import json
import requests
from requests import RequestException
from airflow.models import BaseOperator
import time


class TransparenciaApiReaderOperator(BaseOperator):

    def __init__(self,
                 base_url,
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
                 *args, **kwargs
                 ):

        super(TransparenciaApiReaderOperator, self).__init__(*args, **kwargs)

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

    def __param_generator(self):
        self.log.info("Reading dependent parameters.")
        with open(os.path.join(self.dependency_bucket.format(month=self.month, year=self.year),
                               self.dependency_file.format(month=self.month, year=self.year)), "r") as f:
            data = json.loads(f.read())

        return list(map(lambda item: dict(map(lambda key: (key[1], item.get(key[0])),
                                              self.dependency_params)),
                        data))

    def __fetch_page(self, params, num_retries, sleep_time):
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
        data = []
        while True:
            curr_page_vouchers = self.__fetch_page(parameters, self.num_retries, self.sleep_time)
            if len(curr_page_vouchers) == 0:
                break
            data.extend(curr_page_vouchers)
            curr_page = parameters.get("pagina")
            if curr_page is None:
                break
            parameters.update({"pagina": curr_page + 1})
        return data

    def __process_vouchers(self):
        parameters = {"mesExtratoInicio": f"{self.month}/{self.year}",
                      "mesExtratoFim": f"{self.month}/{self.year}",
                      "pagina": self.pagina}
        return self.__fetch_pages(parameters)

    def __process_organization(self):
        parameters = {"pagina": self.pagina}
        return self.__fetch_pages(parameters)

    def __process_employee(self):
        data = []
        list_params = self.__param_generator()
        for param in list_params:
            param.update({"pagina": self.pagina})
            self.log.info(f"Fetching employees, param: {param}")
            data.extend(self.__fetch_pages(param))
        return data

    def execute(self, context):
        self.month = "{execution_date.month}".format(**context).zfill(2)
        self.year = "{execution_date.year}".format(**context)

        path = os.path.join(self.s3_bucket.format(**context), self.filename.format(month=self.month, year=self.year))
        try:
            os.remove(path)
            self.log.info("Cleaning file contents.")
        except FileNotFoundError:
            pass

        if self.api_endpoint == '/cartoes':
            data = self.__process_vouchers()

        if self.api_endpoint == '/orgaos-siape':
            data = self.__process_organization()

        if self.api_endpoint == '/servidores':
            data = self.__process_employee()

        with open(path, "a") as f:
            f.write(json.dumps(data, ensure_ascii=False))
