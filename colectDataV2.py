import requests
from bs4 import BeautifulSoup
from lxml import etree
from time import time, ctime
from mongo import insert_many, updateLink, getAllLinksNotCollected
import pandas as pd
from threading import Thread
import json
import os
import cronitor

cronitor.api_key = '81ca7a84f7434070a14bd28df00aba61'
cronitor.Monitor.put(
    key='colect-data-job',
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
        links = self.lista
        inicio = time()
        errors = []
        carros = []

        file = open(f"./logs/{self.uf}/thread-{self.id}.log", "w")

        file.write(
            f"{ctime()} - {self.id}{self.uf} - Quantidade de Links: {len(links)}\n")

        count = 0
        for l in links:
            count += 1
            try:
                file.write(f"{ctime()} - {self.id}{self.uf} - Link {count}\n")
                carros.append(coletaDados(l['link']))
            except Exception as e:
                errors.append({
                    'erro': e.args[0],
                    'link': l['link']
                })

        file.write(
            f"{ctime()} - {self.id}{self.uf} - Quantidade de Carros Coletados: {len(carros)}\n")

        if(len(errors) > 0):
            file.write(
                f"{ctime()} - {self.id}{self.uf} - Quantidade de Erros: {len(errors)}\n")
            insert_many(errors, 'errors')

        fim = time()
        file.write(
            f"{ctime()} - {self.id}{self.uf} - Tempo para coletar os dados {round(fim-inicio, 2)} s\n")

        if(len(carros) > 0):
            insert_many(carros, 'cars')
            for c in carros:
                updateLink(c['link'])


def coletaDados(link):
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    }
    result = requests.get(link, headers=header)
    with open("logs/headers.json", "w") as file:
        file.write(json.dumps(dict(result.headers)))

    html = result.content
    soup = BeautifulSoup(html, 'html.parser')

    with open("logs/response.html", "w") as file:
        file.write(soup.prettify())

    if("Anúncio não encontrado | OLX" in soup):
        updateLink(link)
        raise Exception('Anúncio não encontrado')

    dom = etree.HTML(str(soup))
    preco = None
    title = None
    try:
        preco = dom.xpath(
            '/html/body/div[2]/div/div[4]/div[2]/div/div[2]/div[2]/div[15]/div/div/div[1]/div/h2[2]')[0].text
    except:
        updateLink(link)
        raise Exception('Preço não encontrado')
    try:
        title = dom.xpath(
             '/html/body/div[2]/div/div[4]/div[2]/div/div[2]/div[1]/div[19]/div/div/div/div/h1')[0].text
    except:
        updateLink(link)
        raise Exception('Titulo não encontrado')

    opicionaisList = [span.text for span in soup.findAll(
        'span', class_="ad__sc-1g2w54p-0 eDzIHZ sc-ifAKCX cmFKIN")]

    opicionais = {"Vidro elétrico": 0, "Trava elétrica": 0, "Ar condicionado": 0, "Direção hidráulica": 0,
                  "Som": 0, "Air bag": 0, "Alarme": 0, "Sensor de ré": 0, "Câmera de ré": 0, "Blindado": 0}

    for o in opicionaisList:
        if o in opicionais:
            opicionais[o] = 1

    divCaracteristicas = soup.findAll(
        'div', class_="sc-hmzhuo HlNae sc-jTzLTM iwtnNi")

    caracteristicas = {}
    for caracteristica in divCaracteristicas:
        cTitle = caracteristica.find('span', class_='sc-ifAKCX dCObfG').text
        cDesc = None
        if caracteristica.find('a') is None:
            cDesc = caracteristica.find('span', class_='sc-ifAKCX cmFKIN').text
        else:
            cDesc = caracteristica.find('a').text
        caracteristicas[cTitle] = cDesc

    return {
        'title': title,
        'price': preco,
        **caracteristicas,
        **opicionais,
        'uf': link.split(".")[0].split("//")[1],
        'link': link,
        'processed': False
    }


@cronitor.job('colect-data-job')
def main():
    print(ctime())

    contador = 0

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

        diretorio = f'./logs/{uf}'
        if(not os.path.exists(diretorio)):
            os.mkdir(diretorio)

        with open("logs/UF Atual Data.txt", "w") as file:
            file.write(f"Inicio {uf.upper()}")

        inicioTimer = time()

        num_threads = int(os.cpu_count()/2)
        threads = []

        links = getAllLinksNotCollected(uf)
        if(len(links) > num_threads):
            base = int(len(links)/num_threads) + 1
            for i in range(num_threads):
                thread = Th(links[base*i:base*(i+1)], i, uf)
                threads.append(thread)
                contador += len(links[base*i:base*(i+1)])
                pass

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()
        else:
            print(f"Menos de {num_threads} link para coletar")

        fimTimer = time()
        print(
            f"Fim {uf.upper()} -> {round(fimTimer-inicioTimer, 2)} s")

    print(contador)


if __name__ == "__main__":
    main()
