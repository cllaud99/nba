import os
import requests
import pandas as pd

# Definindo a URL e os headers
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
    "&PerMode=PerGame"
    "&Period=0"
    "&PlayerExperience="
    "&PlayerPosition="
    "&PlusMinus=N"
    "&Rank=N"
    "&Season=2023-24"
    "&SeasonSegment="
    "&SeasonType=Playoffs"
    "&ShotClockRange="
    "&StarterBench="
    "&TeamID=0"
    "&VsConference="
    "&VsDivision="
    "&Weight="
)

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

def fetch_api_response(url: str, headers: dict) -> dict:
    """
    Faz uma requisição GET para a API e retorna a resposta em formato JSON.

    Args:
        url (str): URL da API para a qual a requisição será feita.
        headers (dict): Cabeçalhos HTTP para a requisição.

    Returns:
        dict: Resposta da API em formato JSON.
    """
    response = requests.get(url, headers=headers)
    
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
    df = pd.DataFrame(data['resultSets'][0]['rowSet'], columns=data['resultSets'][0]['headers'])
    
    os.makedirs(folder_path, exist_ok=True)
    
    file_path = os.path.join(folder_path, file_name)
    df.to_csv(file_path, index=False)
    print(f"Dados salvos em: {file_path}")

if __name__ == "__main__":
    try:
        data = fetch_api_response(url, headers)
        save_data_to_csv(data, './nba_stats', 'player_stats_2023_24.csv')
    except Exception as e:
        print(e)