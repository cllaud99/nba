from crawler_nba_api import fetch_api_response, save_data_to_csv
from crawler_nba_elements import DropdownScraper
from minio_utils import MinioManager
from loguru import logger
from config_logger import configure_logger

class NBADataProcessor:
    def __init__(self, minio_manager, first_season, url, class_dropdown):
        self.minio_manager = minio_manager
        self.first_season = first_season
        self.url = url
        self.class_dropdown = class_dropdown
        self.season_types = ["Pre Season", "Playoffs", "Regular Season", "All Star", "PlayIn", "IST"]
        
        # Configura o logger
        configure_logger()
        logger.info("Logger configurado com sucesso!")

    def get_seasons(self):
        """
        Obtém os valores das temporadas da página usando DropdownScraper.
        """
        elements = DropdownScraper(self.url, class_dropdown=self.class_dropdown)
        return elements.get_dataframe_values("Season")

    def process_season(self, season):
        """
        Processa dados para uma temporada específica e todos os tipos de temporada.
        """
        logger.info(f"Processando temporada: {season}")
        for season_type in self.season_types:
            logger.info(f"Processando tipo de temporada: {season_type}")
            # Chama o método `main` da classe MinioManager
            self.minio_manager.process_data(
                season,
                season_type,
                "Totals",
                f"nba__{season}_{season_type}_Totals.csv",
                'raw-nba'
            )

    def run(self):
        """
        Executa o processamento de dados.
        """
        df_seasons = self.get_seasons()
        for index, row in df_seasons.iterrows():
            season = row["Value"]
            if season > self.first_season:
                self.process_season(season)
            else:
                logger.warning(f"Temporada {season} não é maior que FIRST_SEASON. Pulando...")

        logger.info("Processamento concluído!")

# Configura o cliente MinIO
minio_manager = MinioManager()

# Constantes
FIRST_SEASON = "1996-97"
URL = "https://www.nba.com/stats/leaders"
CLASS_DROPDOWN = "DropDown_select__4pIg9"

# Cria e executa o processador de dados NBA
nba_processor = NBADataProcessor(minio_manager, FIRST_SEASON, URL, CLASS_DROPDOWN)
nba_processor.run()