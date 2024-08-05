import os

import pandas as pd
import requests

# Definindo os headers para a requisição
headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Host": "stats.nba.com",
    "Referer": "https://stats.nba.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
}


def fetch_api_response(season: str, season_type: str, per_mode: str) -> dict:
    """
    Faz uma requisição GET para a API e retorna a resposta em formato JSON.

    Args:
        season (str): Temporada para a qual a requisição será feita.
        season_type (str): Tipo de temporada (ex: "Regular Season").
        per_mode (str): Modo de métrica (ex: 'PerGame').

    Returns:
        dict: Resposta da API em formato JSON.
    """
    url = (
        "https://stats.nba.com/stats/leaguedashplayerstats"
        "?College="
        "&Conference="
        "&Country="
        "&DateFrom="
        "&DateTo="
        "&Division="
        "&DraftPick="
        "&DraftYear="
        "&GameScope="
        "&GameSegment="
        "&Height="
        "&LastNGames=0"
        "&LeagueID=00"
        "&Location="
        "&MeasureType=Base"
        "&Month=0"
        "&OpponentTeamID=0"
        "&Outcome="
        "&PORound=0"
        "&PaceAdjust=N"
        f"&PerMode={per_mode}"
        "&Period=0"
        "&PlayerExperience="
        "&PlayerPosition="
        "&PlusMinus=N"
        "&Rank=N"
        f"&Season={season}"
        "&SeasonSegment="
        f"&SeasonType={season_type}"
        "&ShotClockRange="
        "&StarterBench="
        "&TeamID=0"
        "&VsConference="
        "&VsDivision="
        "&Weight="
    )
    print(f"URL: {url}")
    response = requests.get(url, headers=headers)

    print(f"Status Code: {response.status_code}")
    print(response.text)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Falha ao recuperar os dados: {response.status_code}")


def save_data_to_csv(data: dict, folder_path: str, file_name: str) -> None:
    """
    Salva os dados em um arquivo CSV.

    Args:
        data (dict): Dados em formato JSON a serem salvos.
        folder_path (str): Caminho da pasta onde o arquivo CSV será salvo.
        file_name (str): Nome do arquivo CSV.

    Returns:
        None
    """
    df = pd.DataFrame(
        data["resultSets"][0]["rowSet"], columns=data["resultSets"][0]["headers"]
    )

    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, file_name)
    df.to_csv(file_path, index=False)
    print(f"Dados salvos em: {file_path}")


if __name__ == "__main__":
    season = "2023-24"
    season_type = "IST"
    per_mode = "Totals"

    try:
        data = fetch_api_response(season, season_type, per_mode)
        save_data_to_csv(
            data, "./nba_stats", f"nba__{season}_{per_mode}_{season_type}.csv"
        )
    except Exception as e:
        print(e)
