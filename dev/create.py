from imserv import db, ImServ

if __name__ == '__main__':
    db.Base.metadata.create_all(ImServ().engine)
