import pandas as pd
import requests
from bs4 import BeautifulSoup
from config_logger import configure_logger
from loguru import logger

# Configura o logger
configure_logger()

class DropdownScraper:
    def __init__(self, url, class_dropdown):
        """
        Inicializa a classe com a URL da página da web e a classe CSS dos dropdowns.

        Args:
            url (str): A URL da página da web que contém os dropdowns.
            class_dropdown (str): A classe CSS dos elementos dropdown a serem encontrados.
        """
        self.url = url
        self.class_dropdown = class_dropdown
        self.dropdown_data = self.get_dropdown_values()

    def _fetch_page(self):
        """
        Faz uma requisição HTTP para obter o conteúdo da página.

        Returns:
            BeautifulSoup: Objeto BeautifulSoup contendo o conteúdo HTML da página.
        """
        try:
            response = requests.get(self.url)
            response.raise_for_status()  # Levanta um erro para códigos de status HTTP 4xx/5xx
            logger.info("Página acessada com sucesso.")
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            logger.error(f"Erro ao acessar a página: {e}")
            return None

    def get_dropdown_values(self):
        """
        Obtém os valores dos dropdowns da página e retorna um dicionário de DataFrames.

        Returns:
            dict: Dicionário onde as chaves são os nomes dos dropdowns e os valores são DataFrames com os valores do dropdown.
        """
        soup = self._fetch_page()
        if soup is None:
            logger.warning("Não foi possível acessar a página. Nenhum valor de dropdown será retornado.")
            return {}

        dropdowns = soup.find_all("select", class_=self.class_dropdown)
        dropdown_data = {}

        if not dropdowns:
            logger.info("Nenhum dropdown encontrado com a classe fornecida.")

        for i, dropdown in enumerate(dropdowns):
            label = dropdown.find_parent("label")
            description = (
                label.find("p").get_text(strip=True) if label else f"Dropdown_{i}"
            )

            values = [
                option.get_text(strip=True)
                for option in dropdown.find_all("option")
                if option.get_text(strip=True)
            ]

            if values:
                df_values = pd.DataFrame(values, columns=["Value"])
                dropdown_data[description] = df_values
                logger.info(f"Valores obtidos para o dropdown '{description}'.")
            else:
                logger.info(f"Sem opções disponíveis para o dropdown {i}.")

        return dropdown_data

    def get_dataframe_values(self, label):
        """
        Retorna o DataFrame correspondente ao label fornecido.

        Args:
            label (str): O rótulo do dropdown para o qual o DataFrame deve ser retornado.

        Returns:
            pd.DataFrame: DataFrame com os valores do dropdown correspondente ao rótulo fornecido.
        """
        df = self.dropdown_data.get(label, pd.DataFrame())
        if df.empty:
            logger.info(f"Nenhum DataFrame encontrado para o label '{label}'.")
        else:
            logger.info(f"DataFrame retornado para o label '{label}'.")
        return df

# Exemplo de uso:
if __name__ == "__main__":
    url = "https://www.nba.com/stats/leaders"
    class_dropdown = "DropDown_select__4pIg9"

    scraper = DropdownScraper(url, class_dropdown)

    label = "Stat Category"
    df = scraper.get_dataframe_values(label)

    if not df.empty:
        print(f"DataFrame para o label '{label}':")
        print(df)
    else:
        print(f"Nenhum DataFrame encontrado para o label '{label}'.")
