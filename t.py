from mongo import getCars, updateCarsMany
from time import sleep

cars = list(getCars())
print(len(cars))

links = []

for car in cars:
    links.append(car['link'])

inicio = 1
fim = 10000
for i in range(int(len(links)/10000)+1):
    linksToUpdate = links[inicio-1:fim]
    print(len(linksToUpdate))

    inicio = fim+1
    fim = fim+10000
    updateCarsMany(links, {'processed': False})
    sleep(1)
