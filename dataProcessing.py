import pandas as pd
from mongo import getCars, getClient, updateCarsMany, deleteCar
from time import ctime, sleep
import schedule
from orm import cnx
import cronitor

cronitor.api_key = '81ca7a84f7434070a14bd28df00aba61'
cronitor.Monitor.put(
    key='data-processing-job',
    type='job',
    schedule='0 * * * *',
)


def updateCar(cars):
    links = []
    for car in cars:
        links.append(car['link'])

    inicio = 1
    fim = 10000
    for i in range(int(len(links)/10000)+1):
        linksToUpdate = links[inicio-1:fim]

        inicio = fim+1
        fim = fim+10000
        updateCarsMany(linksToUpdate, {'processed': True})
        sleep(5)


def removeOutliers(dataset, field):
    series = dataset.groupby('Modelo')[field]
    q1 = series.quantile(.25)
    q3 = series.quantile(.75)
    IIQ = q3 - q1
    limite_inferior = q1 - 1.5 * IIQ
    limite_superior = q3 + 1.5 * IIQ

    series_new = pd.DataFrame()

    for tipo in series.groups.keys():
        eh_tipo = dataset['Modelo'] == tipo
        eh_dentro_limite = (dataset[field] >= limite_inferior[tipo]) & (
            dataset[field] <= limite_superior[tipo])
        selecao = eh_tipo & eh_dentro_limite
        dados_selecao = dataset[selecao]
        series_new = pd.concat([series_new, dados_selecao])

    return series_new


@cronitor.job('data-processing-job')
def main():
    print(ctime())
    cars = list(getCars({"processed": False}))
    toDelete = []
    print(len(cars))
    if(len(cars) == 0):
        print(f"{ctime()} - Nenhum carro a processar")
        return

    for i in range(len(cars)):
        try:
            if(not isinstance(cars[i]['price'], float)):
                cars[i].update(
                    {'price': float(cars[i]['price'].replace('.', ''))})
            if(not isinstance(cars[i]['Quilometragem'], int)):
                cars[i].update({'Quilometragem': int(
                    cars[i]['Quilometragem'].replace('.', ''))})
        except:
            toDelete.append(cars[i]["link"])

    deleteCar(toDelete)

    dataset = pd.DataFrame(cars)

    dataset.drop(columns=['_id', 'link', 'Categoria'], inplace=True)
    dataset.dropna(inplace=True)

    dataset = removeOutliers(dataset, 'price')
    dataset = removeOutliers(dataset, 'Quilometragem')

    dataset.to_csv("output/dataset.csv", index=False)
    dataset.to_sql("cars", cnx, if_exists="append")
    updateCar(cars)

    print(ctime())


if __name__ == "__main__":
    main()
    # getClient()
    # schedule.every().hour.do(main)

    # print(f"Inicio Cron {ctime()}")

    # while 1:
    #     schedule.run_pending()
    #     sleep(60)
