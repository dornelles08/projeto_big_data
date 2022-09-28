from pymongo import MongoClient
import os
from dotenv import load_dotenv
load_dotenv()


host = os.environ['MONGODB_HOST']
user = os.environ['MONGODB_USER']
password = os.environ['MONGODB_PASS']
port = os.environ['MONGODB_PORT']

client = MongoClient(
    f'mongodb://{user}:{password}@{host}:{port}/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false')

db = client.bigData


def getClient():
    return client


def insert_many(array, collection):
    try:
        db[collection].insert_many(array)
    except Exception as e:
        print(e)


def existLink(link):
    existLink = db['links'].find_one({'link': link})
    return True if existLink else False


def getLinksNotCollected(limit=100, skip=0, uf="se"):
    links = list(db['links'].find({'hasCollected': False, "uf": uf}).limit(
        limit).skip(skip))
    return links


def getAllLinksNotCollected(uf="se"):
    links = list(db['links'].find({'hasCollected': False, "uf": uf}))
    return links


def updateLink(link):
    db['links'].update_one({'link': link}, {"$set": {"hasCollected": True}})


def totalLinksNotCollected(uf):
    return db['links'].count_documents({"hasCollected": False, "uf": uf})


def getCars(filters={}):
    return db['cars'].find(filters)


def getCarsByUf(uf):
    return db['cars'].find({"uf": uf})


def updateCarsMany(links, update):
    filter = {"link": {"$in": links}}
    db['cars'].update_many(filter, {"$set": update})


def deleteCar(links):
    db['cars'].delete_many({"link": {"$in": links}})
