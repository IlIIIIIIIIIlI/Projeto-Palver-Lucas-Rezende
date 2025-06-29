from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
from time import sleep
from tqdm import tqdm # mostra a barra de carregamento
from datetime import datetime


def request_url(url, print_status=True):
    bad_response_cnt = 0

    headers = [
        {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15 (Applebot/0.1; +http://www.apple.com/go/applebot) [ip:17.241.227.148]"},
        {"User-Agent": "Mozilla/5.0 (Android 12; Mobile; rv:138.0) Gecko/138.0 Firefox/138.0 [ip:178.197.214.27]"},
        {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15 (Applebot/0.1; +http://www.apple.com/go/applebot) [ip:17.241.227.148]"},
        {"User-Agent": "TuneIn Radio Pro/31.9.0; iPhone17,1; iOS/18.5"}
              ]

    user_message = {
        200: 'URL requisitada com sucesso.',
        201: 'Recurso criado com sucesso.',
        202: 'Requisição aceita para processamento.',
        204: 'Requisição bem-sucedida, sem conteúdo retornado.',
        301: 'Recurso movido permanentemente.',
        302: 'Recurso movido temporariamente.',
        400: 'Requisição inválida.',
        401: 'Não autorizado. Autenticação necessária.',
        403: 'Proibido. Você não tem permissão para acessar este recurso.',
        404: 'Recurso não encontrado.',
        405: 'Método HTTP não permitido para o recurso.',
        408: 'Tempo de requisição esgotado.',
        409: 'Conflito. O recurso já existe ou há um problema de estado.',
        500: 'Erro interno do servidor.',
        501: 'Funcionalidade não implementada no servidor.',
        502: 'Bad Gateway. Erro de comunicação entre servidores.',
        503: 'Serviço indisponível. Tente novamente mais tarde.',
        504: 'Gateway Timeout. Tempo limite ao esperar resposta do servidor.'
    }

    # a cada vez que uma requisição falha, tenta-se novamente com um novo user-agent para evitar bloqueios
    while bad_response_cnt < len(headers) - 1:
        response = requests.get(url, headers=headers[bad_response_cnt])
        requisition_status = response.status_code // 100

        match requisition_status:
            case 2 | 3:
                if print_status:
                    print(user_message[response.status_code])
                return response

            case 4 | 5:
                bad_response_cnt += 1
                sleep(0.5)

    print(user_message[response.status_code])
    return response


def collect_recent_news(data: dict, n=10):
    df = pd.DataFrame(data)
    df['Data (em ISO)'] = pd.to_datetime(df['Data (em ISO)'])

    df.sort_values('Data (em ISO)', ascending=False)

    return df[0:n]


def save_df(df, mode='w'):
    df.to_csv('result.csv',mode=mode, header=False, index=False, encoding='utf-8-sig')
    print('Dados salvos com sucesso.')


def scrape_uol(number_of_news=10):
    """
    Função cujo objetivo é coletar urls e datas a partir do site-maps, e salvar as n notícias mais recentes
    num data frame.
    Dessas n mais recentes, uma nova requisição será feita para coletar o título da matéria.

    Observações: Na página do uol, no head do html estão inclusas as meta tags, o título encontra-se nessa '# <meta property="og:title"'
                 Exemplo: # <meta property="og:title" content="Carla Araújo: Motta surpreende com reviravolta política para queda do IOF">
    """

    target_url = 'https://noticias.uol.com.br/sitemap/v2/today.xml'
    response = request_url(target_url)  # coletando o xml do uol

    soup = bs(response.content, "xml")

    # monto o dicionário contendo as chaves
    news_dict = [
        {
            "Veículo": 'UOL',
            "Link da matéria": url.loc.text,
            "Título da matéria": "",
            "Subtítulo": "",
            "Data (em ISO)": url.lastmod.text
        }
        for url in soup.find_all("url")]

    news_df = collect_recent_news(news_dict, number_of_news)

    print(f'Processando urls - UOL')
    for i in range(len(news_df)):

        response = request_url(news_df['Link da matéria'][i], print_status=False)

        if response:
            soup = bs(response.content, "html.parser")
            title = soup.select('meta[property="og:title"]')[0][
                'content']  # o resultado retornado é uma lista com 1 elemento
            news_df.loc[i, "Título da matéria"] = title

    print('Urls processadas com sucesso.')
    return news_df


def scrape_g1(number_of_news=10):
    """
    Função cujo objetivo é coletar urls e datas a partir do site-maps, e salvar as n notícias mais recentes
    num data frame.
    Dessas n mais recentes, uma nova requisição será feita para coletar o título da matéria.

    Observações: Na página do uol, no head do html estão inclusas as meta tags, o título encontra-se nessa '# <meta property="og:title"'
                 Exemplo: # <meta property="og:title" content="Carla Araújo: Motta surpreende com reviravolta política para queda do IOF">
    """

    target_url = 'https://g1.globo.com/rss/g1/educacao/'
    response = request_url(target_url)  # coletando o xml do uol

    soup = bs(response.content, "xml")

    # monto o dicionário contendo as chaves
    news_dict = [
        {
            "Veículo": 'G1',
            "Link da matéria": item.link.text,
            "Título da matéria": item.title.text,
            "Subtítulo": "",
            "Data (em ISO)": item.pubDate.text
        }
        for item in soup.find_all("item")]

    # Formatar as datas no formato ISO
    print(f'Processando urls - G1')
    for i in range(len(news_dict)):
        date = news_dict[i]['Data (em ISO)']
        date_dt = datetime.strptime(date, '%a, %d %b %Y %H:%M:%S %z')
        date_iso = date_dt.isoformat()
        news_dict[i]['Data (em ISO)'] = date_iso

    news_df = collect_recent_news(news_dict, number_of_news)

    # coletar os subtítulos
    for i in range(len(news_df)):

        response = request_url(news_df['Link da matéria'][i], print_status=False)
        if response.status_code == 200:
            soup = bs(response.content, "html.parser")
            subtitle = soup.select('meta[property="og:description"]')[0][
                'content']  # o resultado retornado é uma lista com 1 elemento
            news_df.loc[i, "Subtítulo"] = subtitle

    print('Urls processadas com sucesso.')

    return news_df

uol_df = scrape_uol()
save_df(uol_df)

g1_df = scrape_g1()
save_df(g1_df, mode='a')
