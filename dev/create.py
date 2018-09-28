from imserv import ImServ


if __name__ == '__main__':
    ims = ImServ()
    ims.init()
    ims.import_images('/Volumes/PATARAPOLW/Exam', skip_hash=True)
