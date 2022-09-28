from mongo import getClient, getCarsByUf
from orm import cnx
import pandas as pd

if __name__ == "__main__":
    print(getClient())
    print(cnx)

    dt = pd.DataFrame([{"nome": "Felipe", "idade": 24},
                       {"nome": "Rafaela", "idade": 17},
                       {"nome": "Carmeluce", "idade": 58}])
    dt.to_sql("teste", cnx, if_exists="replace")
