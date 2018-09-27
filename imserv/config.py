from pathlib import Path

config = {
    'similarity_threshold': 3
}

OS_IMG_FOLDER_PATH = Path.home().joinpath('Pictures')

assert OS_IMG_FOLDER_PATH.exists()

IMG_FOLDER_PATH = OS_IMG_FOLDER_PATH.joinpath('imserv')

IMG_FOLDER_PATH.mkdir(exist_ok=True)
