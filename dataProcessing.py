import pandas as pd
from mongo import getCars, getClient
from time import ctime, sleep
import schedule
import cronitor
from orm import cnx

cronitor.api_key = '81ca7a84f7434070a14bd28df00aba61'
cronitor.Monitor.put(
    key='data-processing-job',
    type='job',
    schedule='0 * * * *',
)


@cronitor.job('data-processing-job')
def main():
    print(ctime())
    cars = list(getCars({'processed': False}))
    for i in range(len(cars)):
        if(not isinstance(cars[i]['price'], float)):
            cars[i].update({'price': float(cars[i]['price'].replace('.', ''))})
        if(not isinstance(cars[i]['Quilometragem'], int)):
            cars[i].update({'Quilometragem': int(
                cars[i]['Quilometragem'].replace('.', ''))})

    dataset = pd.DataFrame(cars)

    dataset.drop(columns=['_id', 'link', 'Categoria'], inplace=True)
    dataset.dropna(inplace=True)

    price = dataset.groupby('Modelo')['price']
    q1 = price.quantile(.25)
    q3 = price.quantile(.75)
    IIQ = q3 - q1
    limite_inferior = q1 - 1.5 * IIQ
    limite_superior = q3 + 1.5 * IIQ

    price_new = pd.DataFrame()

    for tipo in price.groups.keys():
        eh_tipo = dataset['Modelo'] == tipo
        eh_dentro_limite = (dataset['price'] >= limite_inferior[tipo]) & (
            dataset['price'] <= limite_superior[tipo])
        selecao = eh_tipo & eh_dentro_limite
        dados_selecao = dataset[selecao]
        price_new = pd.concat([price_new, dados_selecao])

    quilometragem = price_new.groupby('Modelo')['Quilometragem']
    q1 = quilometragem.quantile(.25)
    q3 = quilometragem.quantile(.75)
    IIQ = q3 - q1
    limite_inferior = q1 - 1.5 * IIQ
    limite_superior = q3 + 1.5 * IIQ

    quilometragem_new = pd.DataFrame()

    for tipo in quilometragem.groups.keys():
        eh_tipo = price_new['Modelo'] == tipo
        eh_dentro_limite = (price_new['Quilometragem'] >= limite_inferior[tipo]) & (
            price_new['Quilometragem'] <= limite_superior[tipo])
        selecao = eh_tipo & eh_dentro_limite
        dados_selecao = price_new[selecao]
        quilometragem_new = pd.concat([quilometragem_new, dados_selecao])

    dataset = quilometragem_new.copy()

    dataset.to_csv("output/dataset.csv", index=False)
    dataset.to_sql("cars", cnx, if_exists="replace")
    print(ctime())


if __name__ == "__main__":
    getClient()
    schedule.every().hour.do(main)

    print(f"Inicio Cron {ctime()}")

    while 1:
        schedule.run_pending()
        sleep(60)
