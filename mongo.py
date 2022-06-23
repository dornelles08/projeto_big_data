from pymongo import MongoClient

host = "mongodb"

client = MongoClient(
    f'mongodb://root:pass@{host}:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false')

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
