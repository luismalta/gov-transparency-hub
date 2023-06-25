import re
import time
import json
import requests


class PortalTransparenciaScrapper():
    def __init__(self, city):
        self.city = city

        self.thread_sha_token = None
        self.report_thread_id = None
        self.report_id = None
        self.generated_report = ''

        self.BASE_URL = f'https://ptn.{self.city}.mg.gov.br'

    def get_report(self, report_name, report_format, report_arguments):
        self._get_thread_token(report_name)
        self._start_report_generate_thread(report_arguments)
        self._wait_to_generate_report_id()
        self._convert_report_format(report_format)

        return self.generated_report

    def _get_thread_token(self, report_name):
        # Pega SHA para a thread de consulta do relatório
        url = self.BASE_URL + f'/{report_name}'
        response = requests.request("GET", url)
        self.thread_sha_token = re.findall('&SHA.*&INT_TOKEN.*"', response.text)[0][:-1]

    def _start_report_generate_thread(self, report_arguments):
        # Inicia thread para gerar relatório
        url = self.BASE_URL + f'/gerar_relatorio.php?Data={time.time()}{self.thread_sha_token}'

        payload = "&".join(report_arguments)
        headers = {
            'content-type': 'application/x-www-form-urlencoded',
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        self.report_thread_id = re.findall(r'- (\d+)', response.text)

    def _wait_to_generate_report_id(self):
        # Checa se a thread de consulta foi finalizada
        flag = '002 - A thread ainda está em execução.'
        while flag == "002 - A thread ainda está em execução.":
            url = self.BASE_URL + f'/Aguarda_Resultado_Thread.php?INT_THREAD={self.report_thread_id}&DataHora={time.time()}'
            response = requests.request("GET", url)
            flag = response.text
            print(response.text)
            time.sleep(3)

        self.report_id = re.findall(r'- /Dados/(.*).html', response.text)[0]
   
    def _convert_report_format(self, report_format):
        url = self.BASE_URL + f'/converterPara.php?NM_ARQ={self.report_id}&FMT={report_format.upper()}'
        response = requests.request("GET", url)
        converted_report_id = json.loads(response.text)['NM_ARQ_FIM']

        url = self.BASE_URL + converted_report_id
        response = requests.request("GET", url)
        response.encoding = response.apparent_encoding
        self.generated_report = response.text
