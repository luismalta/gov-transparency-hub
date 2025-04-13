import re
import time
import json
import requests
import unicodedata
import pandas as pd
from bs4 import BeautifulSoup

from .constants import (
    URLS,
    DETAILED_EXPENSE_FIELDS,
    ITEM_EXPENSE_FIELDS,
    INVOICE_EXPENSE_FIELDS,
)


class PortalTransparenciaScrapper:
    def __init__(self, city):
        self.city = city

        self.thread_sha_token = None
        self.report_thread_id = None
        self.report_id = None
        self.generated_report = ""

        self.BASE_URL = URLS.get(self.city)

    def get_report(self, report_name, report_format, report_arguments):
        """
        Generates a report based on the provided parameters.

        Args:
            report_name (str): The name of the report to be generated.
            report_format (str): The format in which the report should be generated (e.g., 'pdf', 'csv').
            report_arguments (dict): A dictionary of arguments required for generating the report.

        Returns:
            generated_report: The generated report in the specified format.
        """
        self._get_thread_token(report_name)
        self._start_report_generate_thread(report_arguments)
        self._wait_to_generate_report_id()
        self._convert_report_format(report_format)

        return self.generated_report

    def _get_thread_token(self, report_name):
        # Get SHA for the report query thread
        url = self.BASE_URL + f"/{report_name}"
        response = requests.request("GET", url, verify=False)
        self.thread_sha_token = re.findall('&SHA.*&INT_TOKEN.*"', response.text)[0][:-1]

    def _start_report_generate_thread(self, report_arguments):
        # Start thread to generate report
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
        # Check if the query thread has finished
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
        if report_format != "html":
            url = (
                self.BASE_URL
                + f"/converterPara.php?NM_ARQ={self.report_id}&FMT={report_format.upper()}"
            )
            response = requests.request("GET", url, verify=False)
            converted_report_id = json.loads(response.text)["NM_ARQ_FIM"]
            url = self.BASE_URL + converted_report_id
        else:
            url = self.BASE_URL + f"/Dados/{self.report_id}.html"

        response = requests.request("GET", url, verify=False)
        response.encoding = response.apparent_encoding
        self.generated_report = response.text

    def extract_revenue_deatils(self, revenue_html_report):
        """
        Extracts revenue details from a HTML report.

        Args:
            revenue_html_report (str): The HTML content of the revenue report.

        Returns:
            list: A list of dictionaries, each containing the extracted revenue details.
        """
        soup_page = BeautifulSoup(
            revenue_html_report, "html.parser", from_encoding="UTF-8"
        )

        table = soup_page.find_all("table")
        revenue_df = pd.read_html(str(table))[0]

        revenue_df.query(
            'not Data.str.contains("TOTAL")', engine="python", inplace=True
        )
        revenue_df.query(
            'not Data.str.contains("Total do Dia")', engine="python", inplace=True
        )

        revenue_df["id"] = revenue_df.apply(lambda x: time.time(), axis=1)

        return revenue_df

    def get_expense_details_pages(self, expense_html):
        """
        Retrieves the expense details pages from the provided HTML content.

        Args:
            expense_html (str): The HTML content containing expense details.

        Returns:
            list: A list of downloaded expense details pages.
        """

        expense_details_urls = self._extract_expense_details_urls(expense_html)
        return self._download_expense_details_page(expense_details_urls)

    def _extract_expense_details_urls(self, expense_html):
        soup_page = BeautifulSoup(expense_html, "html.parser", from_encoding="UTF-8")

        href_elemets = soup_page.find_all("a", title="Clique para obter mais detalhes")

        expense_details_urls = []
        for element in href_elemets:
            expense_details_urls.append(
                re.search("javascript:abrir\('(.*)'\)", element["href"]).group(1)
            )

        return expense_details_urls

    def _download_expense_details_page(self, expense_details_urls):
        expense_details = []
        for expense_url in expense_details_urls:
            url = self.BASE_URL + expense_url
            response = requests.request("GET", url, verify=False)
            expense_details.append(
                {"url": expense_url, "html_content": response.content}
            )
        return expense_details

    def extract_expenses_details(self, expense_details_pages):
        """
        Extracts detailed expense information from a list of HTML pages.
        Args:
            expense_details_pages (list of str): A list of HTML page contents as strings,
                             each containing details of an expense.
        Returns:
            list of dict: A list of dictionaries where each dictionary contains detailed
                  information about an expense. The keys in the dictionary include:
                  - 'numero': The expense number.
                  - 'ano': The expense year.
                  - Other keys corresponding to fields defined in DETAILED_EXPENSE_FIELDS.
                  - 'Referencia': The expense reference.
        """
        expense_details = []
        for details_page in expense_details_pages:
            soup_page = BeautifulSoup(
                details_page, "html.parser", from_encoding="UTF-8"
            )

            if self._expense_is_missing(soup_page):
                expense_details.append(
                    self._fill_missing_expense_with_dummy_values(DETAILED_EXPENSE_FIELDS + ["Referencia", "numero", "ano"])
                )
                continue

            details = {
                "numero": self._extract_expense_number(soup_page),
                "ano": self._extract_expense_year(soup_page),
            }
            for field_name in DETAILED_EXPENSE_FIELDS:
                field = soup_page.find("td", string=re.compile(field_name + ":"))
                field_value = field.text if field else None

                field_name = unicodedata.normalize("NFKD", field_name)

                if field_value:
                    field_value = unicodedata.normalize("NFKD", field_value)
                    field_value = field_value.replace(field_name + ": ", "")

                details[field_name] = field_value

            details["Referencia"] = self._extract_expense_reference(soup_page)

            expense_details.append(details)

        return expense_details

    def _extract_expense_number(self, soup_page):
        tds = soup_page.find_all("td")
        td_result = None

        for td in tds:
            if "Número" in td.get_text():
                td_result = td
                break

        if td_result:
            bold_content = (
                td_result.find("b").get_text(strip=True)
                if td_result.find("b")
                else None
            )
            return bold_content

    def _extract_expense_year(self, soup_page):
        year_element = soup_page.find("td", string=re.compile("Exercício"))

        return year_element.get_text()[-4:]

    def _extract_expense_reference(self, soup_page):
        # Extract reference, with exist
        td = next(
            (t for t in soup_page.find_all("td") if "Referência:" in t.get_text()), None
        )

        if td:
            b_tag = td.find("b")
            if b_tag:
                return b_tag.text
        else:
            return None

    def extract_expense_itens(self, expense_details_pages):
        """
        Extracts expense items from a list of expense details pages.
        Args:
            expense_details_pages (list of str): A list of HTML content strings, each representing an expense details page.
        Returns:
            list of dict: A list of dictionaries, each containing the extracted expense item details. Each dictionary includes:
            - 'item': The item name.
            - 'expense_number': The expense number extracted from the page.
            - 'expense_year': The expense year extracted from the page.
            - Additional fields as defined in ITEM_EXPENSE_FIELDS, with their corresponding values.
        """
        expense_itens = []
        for details_page in expense_details_pages:
            soup_page = BeautifulSoup(
                details_page, "html.parser", from_encoding="UTF-8"
            )

            if self._expense_is_missing(soup_page):
                expense_itens.append(
                    self._fill_missing_expense_with_dummy_values(ITEM_EXPENSE_FIELDS + ["item", "expense_number", "expense_year"])
                )
                continue

            item_elements = soup_page.find_all(
                "th", colspan=re.compile("3"), string=re.compile("Item")
            )
            for item in item_elements:
                item_map = {
                    "item": item.text,
                    "expense_number": self._extract_expense_number(soup_page),
                    "expense_year": self._extract_expense_year(soup_page),
                }

                for field_name in ITEM_EXPENSE_FIELDS:
                    field = item.find_next("td", string=re.compile(field_name + ":"))
                    field_value = field.text if field else None

                    field_name = unicodedata.normalize("NFKD", field_name)

                    if field_value:
                        field_value = unicodedata.normalize("NFKD", field_value)
                        field_value = field_value.replace(field_name + ": ", "")

                    item_map[field_name] = field_value
                expense_itens.append(item_map)
        return expense_itens

    def extract_expense_invoice(self, expense_details_pages):
        """
        Extracts expense invoice details from a list of expense details pages.
        Args:
            expense_details_pages (list of str): A list of HTML content strings representing expense details pages.
        Returns:
            list of dict: A list of dictionaries where each dictionary contains the extracted invoice details.
                Each dictionary has the following keys:
                    - 'codigo': The code of the invoice.
                    - 'tipo': The type of the invoice.
                    - 'expense_number': The expense number extracted from the page.
                    - 'expense_year': The expense year extracted from the page.
                    - Additional fields as defined in INVOICE_EXPENSE_FIELDS, with their corresponding values.
        """
        expense_invoices = []
        for details_page in expense_details_pages:
            soup_page = BeautifulSoup(
                details_page, "html.parser", from_encoding="UTF-8"
            )

            if self._expense_is_missing(soup_page):
                expense_invoices.append(
                    self._fill_missing_expense_with_dummy_values(INVOICE_EXPENSE_FIELDS + ["codigo", "tipo", "expense_number", "expense_year"])
                )
                continue

            invoice_element = soup_page.find("td", string=re.compile("Nota Fiscal"))

            if invoice_element:
                invoice = {
                    "codigo": invoice_element.find_next(
                        "td", string=re.compile("Código .*")
                    ).get_text(),
                    "tipo": invoice_element.find_next(
                        "td", string=re.compile("Tipo .*")
                    ).get_text(),
                    "expense_number": self._extract_expense_number(soup_page),
                    "expense_year": self._extract_expense_year(soup_page),
                }

                for field_name in INVOICE_EXPENSE_FIELDS:
                    field = invoice_element.find_next(
                        "td", string=re.compile(field_name + ":")
                    )
                    field_value = field.text if field else None

                    field_name = unicodedata.normalize("NFKD", field_name)

                    if field_value:
                        field_value = unicodedata.normalize("NFKD", field_value)
                        field_value = field_value.replace(field_name + ": ", "")

                    invoice[field_name] = field_value
                expense_invoices.append(invoice)
        return expense_invoices
    
    def _expense_is_missing(self, soup_page):
        body_text = soup_page.body.get_text()
        if "Não foi possível mostrar as informações deste portal" in body_text:
            return True
        return False

    def _fill_missing_expense_with_dummy_values(self, fields):
        record = {}
        for field in fields:
            record[field] = "N/A"
        return record


