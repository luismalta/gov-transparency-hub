import re
import time
import json
import requests
import unicodedata
from bs4 import BeautifulSoup

from .constants import URLS, DETAILED_EXPENSE_FIELDS


class PortalTransparenciaScrapper:
    def __init__(self, city):
        self.city = city

        self.thread_sha_token = None
        self.report_thread_id = None
        self.report_id = None
        self.generated_report = ""

        self.BASE_URL = URLS.get(self.city)

    def get_report(self, report_name, report_format, report_arguments):
        self._get_thread_token(report_name)
        self._start_report_generate_thread(report_arguments)
        self._wait_to_generate_report_id()
        self._convert_report_format(report_format)

        return self.generated_report

    def _get_thread_token(self, report_name):
        # Pega SHA para a thread de consulta do relatório
        url = self.BASE_URL + f"/{report_name}"
        response = requests.request("GET", url, verify=False)
        self.thread_sha_token = re.findall('&SHA.*&INT_TOKEN.*"', response.text)[0][:-1]

    def _start_report_generate_thread(self, report_arguments):
        # Inicia thread para gerar relatório
        url = (
            self.BASE_URL
            + f"/gerar_relatorio.php?Data={time.time()}{self.thread_sha_token}"
        )

        payload = "&".join(report_arguments)
        headers = {
            "content-type": "application/x-www-form-urlencoded",
        }
        response = requests.request(
            "POST", url, headers=headers, data=payload, verify=False
        )
        self.report_thread_id = re.findall(r"- (\d+)", response.text)

    def _wait_to_generate_report_id(self):
        # Checa se a thread de consulta foi finalizada
        flag = "002 - A thread ainda está em execução."
        while flag == "002 - A thread ainda está em execução.":
            url = (
                self.BASE_URL
                + f"/Aguarda_Resultado_Thread.php?INT_THREAD={self.report_thread_id}&DataHora={time.time()}"
            )
            response = requests.request("GET", url, verify=False)
            flag = response.text
            print(response.text)
            time.sleep(3)

        self.report_id = re.findall(r"- /Dados/(.*).html", response.text)[0]

    def _convert_report_format(self, report_format):
        url = (
            self.BASE_URL
            + f"/converterPara.php?NM_ARQ={self.report_id}&FMT={report_format.upper()}"
        )
        response = requests.request("GET", url, verify=False)
        converted_report_id = json.loads(response.text)["NM_ARQ_FIM"]

        url = self.BASE_URL + converted_report_id
        response = requests.request("GET", url, verify=False)
        response.encoding = response.apparent_encoding
        self.generated_report = response.text

    def get_detailed_expense(self, expense_number, year):

        url = (
            self.BASE_URL
            + f"/Relatorios/Detalhamento_Despesa.php?ID8_DESP={expense_number}&STR_EXR_EXR={year}&CHAR_ID_EMP=1&LG_OP_DESP=S"
        )
        response = requests.request("GET", url, verify=False)
        soup_page = BeautifulSoup(response.content, "html.parser", from_encoding="UTF-8")

        # TODO: Understand the LG_OP_DESP variable so as not to need this brute force check
        if soup_page.find("p", string=re.compile("Despesa não encontrada.")):
            url = (
                self.BASE_URL
                + f"/Relatorios/Detalhamento_Despesa.php?ID8_DESP={expense_number}&STR_EXR_EXR={year}&CHAR_ID_EMP=1&LG_OP_DESP=N"
            )
            response = requests.request("GET", url, verify=False)
            soup_page = BeautifulSoup(
                response.content, "html.parser", from_encoding="UTF-8"
            )

        detailed_expense = {
            "Número": expense_number,
            "Exercício": year,
        }
        for field_name in DETAILED_EXPENSE_FIELDS:
            field = soup_page.find("td", string=re.compile(field_name + ":"))
            field_value = field.text if field else "-"
            field_value = unicodedata.normalize("NFKD", field_value)
            field_name = unicodedata.normalize("NFKD", field_name)
            detailed_expense[field_name] = field_value.replace(field_name + ": ", "")

        return detailed_expense
