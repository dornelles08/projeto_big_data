from random import randint
from time import sleep, ctime
from colectLinks import coletaLinks
from threading import Thread


class Th(Thread):

    def __init__(self, lista, id, uf):
        Thread.__init__(self)
        self.lista = lista
        self.id = id
        self.ufs = uf

    def run(self):
        stop = randint(0, 10)
        sleep(stop)
        print(f"{self.id} - {self.ufs} -> {self.lista}")


def main(uf=0):
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

    print(ctime())
    for uf in ufs:
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

    print(ctime())


if __name__ == "__main__":
    main()
