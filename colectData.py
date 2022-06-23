# import schedule
import requests
from bs4 import BeautifulSoup
from lxml import etree
from time import time, ctime, sleep
from mongo import getLinksNotCollected, insert_many, updateLink, totalLinksNotCollected
import pandas as pd
from threading import Thread
from alive_progress import alive_bar
import json


class Th(Thread):

    def __init__(self, lista):
        Thread.__init__(self)
        self.lista = lista

    def run(self):
        links = self.lista
        inicio = time()
        errors = []
        carros = []

        for l in links:
            try:
                carros.append(coletaDados(l['link']))
            except Exception as e:
                errors.append({
                    'erro': e.args[0],
                    'link': l['link']
                })

        print(f"Quantidade de Erros: {len(errors)}")
        if(len(errors) > 0):
            insert_many(errors, 'errors')

        fim = time()
        print(f"Tempo para coletar os dados {round(fim-inicio, 2)} s")
        print(
            f"Tempo médio para coletar os dados {round(fim-inicio, 2)/len(links)} s")

        inicio = time()
        insert_many(carros, 'cars')
        fim = time()
        print(f"Tempo para inserir os dados no Mongo {round(fim-inicio, 2)} s")


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
        'link': link
    }


def main():
    print(ctime())
    limit = 10
    skip = 0
    uf = "se"
    totalLinks = totalLinksNotCollected(uf)
    with alive_bar(int(totalLinks/limit)+1) as bar:
        while(True):
            count = 0
            links = getLinksNotCollected(limit, skip, uf)
            # skip += limit
            print(f"Quantidade de Links: {len(links)}")

            if(len(links) < 1):
                print(ctime())
                break

            errors = []
            carros = []

            for l in links:
                count += 1
                print(f"Link {count}")
                try:
                    carros.append(coletaDados(l['link']))
                except Exception as e:
                    print(e.args[0])
                    errors.append({
                        'erro': e.args[0],
                        'link': l['link'],
                        'uf': uf
                    })

            print(f"Quantidade de Carros Coletados: {len(carros)}")

            if(len(errors) > 0):
                # print(f"Quantidade de Erros: {len(errors)}")
                insert_many(errors, 'errors')

            if(len(carros) > 0):
                insert_many(carros, 'cars')
                file = "./output/carros.csv"
                df = pd.read_csv(file)
                df = pd.concat([df, pd.DataFrame(carros)], ignore_index=True)
                df.to_csv(file, index=False)
                for c in carros:
                    updateLink(c['link'])

            bar()
            sleep(1)


if __name__ == "__main__":
    main()
