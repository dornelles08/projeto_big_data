from random import random
from threading import Thread
from colectLinks import coletaLinks
from time import sleep


class Th(Thread):

    def __init__(self, lista, id):
        Thread.__init__(self)
        self.lista = lista
        self.id = id

    def run(self):
        tempo = random()
        sleep(tempo)
        print(f"{self.id} - {tempo} - {self.lista}")
        # coletaLinks(self.lista, "se")


pages_to_collect = list(range(1, 101))
inicio = 1
fim = 10
threads = []
for i in range(1, 11):
    a = Th(pages_to_collect[inicio-1:fim], i)
    threads.append(a)
    inicio = fim+1
    fim = fim+10

for t in threads:
    t.start()
for t in threads:
    t.join()

print("Fim")
