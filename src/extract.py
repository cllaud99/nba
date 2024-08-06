from crawler_nba_api import fetch_api_response, save_data_to_csv
from crawler_nba_elements import DropdownScraper
from minio_utils import main
from loguru import logger
from config_logger import configure_logger


configure_logger()
logger.info("Logger configurado com sucesso!")

# Constantes
FIRST_SEASON = "1996-97"
url = "https://www.nba.com/stats/leaders"
class_dropdown = "DropDown_select__4pIg9"

# Obter dados dos elementos da página
elements = DropdownScraper(url, class_dropdown=class_dropdown)
df_seasons = elements.get_dataframe_values("Season")

season_types = ["Pre Season", "Playoffs", "Regular Season", "All Star", "PlayIn", "IST"]

# Processar cada temporada e tipo de temporada
for index, row in df_seasons.iterrows():
    season = row["Value"]
    if season > FIRST_SEASON:
        logger.info(f"Processando temporada: {season}")
        for season_type in season_types:
            logger.info(f"Processando tipo de temporada: {season_type}")
            main(season, season_type, "Totals", f"nba__{season}_{season_type}_Totals.csv", 'raw-nba')
    else:
        logger.warning(f"Temporada {season} não é maior que FIRST_SEASON. Pulando...")

logger.info("Processamento concluído!")