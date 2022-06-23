import requests
from bs4 import BeautifulSoup
from time import time, ctime, sleep
from mongo import insert_many, existLink
from threading import Thread
import cronitor

cronitor.api_key = '81ca7a84f7434070a14bd28df00aba61'
cronitor.Monitor.put(
    key='colect-links-job',
    type='job',
    schedule='0 * * * *',
)


class Th(Thread):

    def __init__(self, lista, id, uf):
        Thread.__init__(self)
        self.lista = lista
        self.id = id
        self.uf = uf

    def run(self):
        coletaLinks(self.lista, self.uf)


def coletaLinksPage(page, uf):
    header = {
        'Host': f'{uf}.olx.com.br',
        'Connection': 'close',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7'
    }

    result = requests.get(
        f"https://{uf}.olx.com.br/autos-e-pecas/carros-vans-e-utilitarios?o="+str(page), headers=header)

    html = result.content
    soup = BeautifulSoup(html, 'html.parser')

    itens = soup.find_all(id="ad-list")

    links = []

    for item in itens:
        carros = item.find_all('a')
        for carro in carros:
            link = carro.get('href')
            if not existLink(link):
                links.append({'link': link, 'uf': uf, 'hasCollected': False})

    return links


def coletaLinks(lista, uf):
    inicio = time()
    totalLinks = []
    for i in lista:
        links = coletaLinksPage(i, uf)
        for f in links:
            totalLinks.append(f)
        sleep(1)

    # print(f"Total de links a coletados {uf}: {len(totalLinks)}")

    fim = time()

    # print(f"Tempo para coletar os links {round(fim-inicio, 2)} s")

    if(len(totalLinks) > 0):
        insert_many(totalLinks, 'links')


@cronitor.job('colect-links-job')
def main():
    print(ctime())
    pages_to_collect = list(range(1, 101))
    num_threads = 10
    ufs = ["ac",
           "al",
           "ap",
           "am",
           "ba",
           "ce",
           "df",
           "es",
           "go",
           "ma",
           "mt",
           "ms",
           "mg",
           "pa",
           "pb",
           "pr",
           "pe",
           "pi",
           "rj",
           "rn",
           "rs",
           "ro",
           "rr",
           "sc",
           "sp",
           "se",
           "to"]

    for uf in ufs:
        print(f"Inicio {uf.upper()}")
        inicioTimer = time()
        threads = []
        inicio = 1
        fim = 10
        for i in range(num_threads):
            thread = Th(pages_to_collect[inicio-1:fim], i, uf)
            threads.append(thread)
            inicio = fim+1
            fim = fim+10

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        fimTimer = time()

        print(
            f"Fim {uf.upper()} -> {round(fimTimer-inicioTimer, 2)} s")

    print(ctime())


if __name__ == "__main__":
    main()
