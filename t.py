from mongo import getCars
from time import ctime, sleep
import pandas as pd
from orm import cnx


print(ctime())

dataset = pd.read_sql('SELECT * FROM cars', cnx)

print(dataset.shape)

# dataset.to_csv("output/dataset.csv", index=False)
# dataset.to_sql("cars", cnx, if_exists="replace")


print(ctime())
