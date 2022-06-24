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
        'Host': 'se.olx.com.br',
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
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cookie': 'r_id=bcf87d4d-2fc5-4711-a6f8-61dc56c811eb; Domain=.olx.com.br; Path=/; Max-Age=315360000, TestAB_Groups=abuy-list-item-fair-badge_orange.ads-exp-qualtrics_enabled.adv-728x90-home_homeB.adv-adagio_enabled.adv-adview-afsh-native_enabled.adv-adview-lazyload_enabled.adv-afs_search.adv-bad-bidders_disabled.adv-billboard_enabled.adv-chat_enabled.adv-gemini_enabled.adv-has-adblocker_enabled.adv-insureads_enabled.adv-listing-afsh-native_enabled.adv-listing-afsh_enabled.adv-magnite-adview_enabled.adv-magnite-listing_enabled.adv-monitoring_enabled.adv-seasonal_control.ai-ds-v2_control.banner-0de6edb35ad54b95bc06a10e727d1b07-web_show.bj-transactional-info_enabled.central-ajuda-banner-incident_A.central-ajuda-pro-sem-chat_control.chatmod-banner_on-without-first-tip.ck-filters-motos_control.dado-adicional-admod-web_enabled.delivery-disputes-help-section_A.delivery-disputes-my-mediation-section_A.delivery-minhas-compras-e-vendas-menu_enable-novo.dispute-form-improved_A.disputes-after-sales-contact-us-button_A.disputes-subreason-diagnose_A.email-confirmation_yes.financing-async-simulations_enabled.grayzone-facematch_on.help-center-new-pre-chat_A.helpcenter-chat-intercom_A.helpcenter-chat-salesforce_A.helpcenter-chat_A.helpcenter-logged-experience_A.helpcenter-my-purchases_A.helpcenter-new-categories_A.helpcenter-selfservice-pending-publishing_A.helpcenter-v2_A.hug-fale-conosco-thumbs-down_A.imo-card-sug-oi_on.imo-xp-dynamic-map_control.ml-rollout-fast-resolution_control.modal-gestao-de-contatos-web_enabled.nu-experiment-test_show.nubanner-web_show.olx-central-header_enabled.osp-new-front-web_new-front.osp-newpos-bundles_landing-v2.payg-disable-bronze-highlight_enabled.payg-discount-julius_control.payg-discount-re-julius_0-1-40.payg-myads-discount_40.pos-new-autos-plans-subscription_control.pp-myplan-header-button_show.removalAdOnboard_A.search-input-header_control.seller-rating_enabled.telefone-descricao-anuncio_showsecuritytip1.txp-chat-negotiation-buyers_control.txp-chat-negotiation-sellers_control.unreadable-category-hide_enabled.vasaut-card-sug-oi_on.vasaut-purchase-intention-question_required-question.vasimo-card-suggestion_enabled.vx-myads-insertion_modal.vx-tag-listing_active.vx-videos-on-adview-gallery_second-position.whatsapp-na-central-de-ajuda_A; Domain=.olx.com.br; Path=/; Max-Age=1800'
        # 'Cookie': 'r_id=aaf46dbf-8815-4e2b-8003-49f15e226c34; _gcl_au=1.1.158857247.1649090147; _rtbhouse_source_=organic; nl_id=3012f6bc-9999-4864-a9c3-e4315be61d9a; _ga_50C013M2CC=GS1.1.1655476506.14.1.1655476978.0; _ga=GA1.3.1959075456.1649090147; _tt_enable_cookie=1; _ttp=6f41694e-c9d4-4eca-991e-d12b547c9297; l_id=3a155bd8-4b3c-41b4-b739-ba487377c8b1; _hjid=3b5352cb-bb16-47c8-9af9-4a1f90a5a35d; _hjSessionUser_736533=eyJpZCI6ImZhZmIwYTBiLTRkODAtNTI4OS1hMTlmLTczODY4NjkwOWRmOSIsImNyZWF0ZWQiOjE2NDkwOTAxNTI1NTIsImV4aXN0aW5nIjp…ea6fb9; tt.u=0100007F8B585461CA063C2B02B9673C; __gsas=ID=d58c1e83d6c79d20:T=1651497190:S=ALNI_MZO8SKfk4av8Q7FXDyyd2S6w4ogfg; _hjSessionUser_1425418=eyJpZCI6ImFkZWMyOTZjLTk4YzktNTUxMC1iYWViLTZiNGJhMWQwNWQyOSIsImNyZWF0ZWQiOjE2NTIzOTgyMDUyMDQsImV4aXN0aW5nIjp0cnVlfQ==; userID=; __gsas=ID=4befda5541bdd569:T=1654224097:S=ALNI_Ma1I8N2h7Hv6tYWIpHCA2ccBTXBsg; pbjs_sharedId=88e3edc7-823e-4fe5-ac75-5389141bd839; _pbjs_userid_consent_data=3524755945110770; _clck=k82axx|1|f2e|0; _uetvid=42985d10eca711ec92683b285f6e072d'

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
            '/html/body/div[2]/div/div[4]/div[2]/div/div[2]/div[2]/div[17]/div/div/div[1]/div[2]/h2[2]')[0].text
    except:
        updateLink(link)
        raise Exception('Preço não encontrado')
    try:
        title = dom.xpath(
            '/html/body/div[2]/div/div[4]/div[2]/div/div[2]/div[1]/div[17]/div/div/div/div/h1')[0].text
    except:
        updateLink(link)
        raise Exception('Titulo não encontrado')

    opicionaisList = [span.text for span in soup.findAll(
        'span', class_="sc-1g2w54p-0 bCoOvQ sc-ifAKCX cmFKIN")]

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
