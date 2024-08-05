from crawler_nba_api import fetch_api_response, save_data_to_csv
from crawler_nba_elements import DropdownScraper
from minio_utils import main


FIRST_SEASON = "1996-97"
url = "https://www.nba.com/stats/leaders"
class_dropdown = "DropDown_select__4pIg9"

elements = DropdownScraper(url, class_dropdown=class_dropdown)
df_seasons = elements.get_dataframe_values("Season")

season_types = ["Pre Season", "Playoffs", "Regular Season", "All Star", "PlayIn", "IST"]

for index, row in df_seasons.iterrows():
    season = row["Value"]
    if season > FIRST_SEASON:
        print(season)
        for type in season_types:
            print(type)
            main(season, type, "Totals", f"nba__{season}_{type}_Totals.csv", 'raw-nba')
    else:
        print(f"Season {season} is not greater than FIRST_SEASON. Skipping...")
