from threading import Thread
from colectLinks import coletaLinks


class Th(Thread):

    def __init__(self, lista):
        Thread.__init__(self)
        self.lista = lista

    def run(self):
        coletaLinks(self.lista, "se")


pages_to_collect = list(range(1, 101))
inicio = 1
fim = 10
for i in range(1, 11):
    a = Th(pages_to_collect[inicio-1:fim])
    a.start()
    inicio = fim+1
    fim = fim+10
