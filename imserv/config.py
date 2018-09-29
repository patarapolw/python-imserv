import os
from pathlib import Path

config = {
    'engine': 'postgresql://localhost/imserv',
    'host': 'localhost',
    'port': 8000,
    'debug': False,
    'threaded': False,
    'hash_size': 32,
    'hash_difference_threshold': 0,
    'skip_hash': False,
    'folder': None
}

for k in config.keys():
    env_k = 'IMSERV_' + k.upper()
    if env_k in os.environ.keys():
        v = os.environ[env_k]

        if k in {'port', 'hash_size', 'hash_difference_threshold', 'skip_hash'}:
            config[k] = int(v)
        elif k in {'debug', 'threaded'}:
            config[k] = bool(v)
        else:
            config[k] = v

if config['folder'] is None:
    OS_IMG_FOLDER_PATH = Path.home().joinpath('Pictures')
    assert OS_IMG_FOLDER_PATH.exists()
    IMG_FOLDER_PATH = OS_IMG_FOLDER_PATH.joinpath('imserv')
    IMG_FOLDER_PATH.mkdir(exist_ok=True)
    config['folder'] = IMG_FOLDER_PATH
else:
    config['folder'] = Path(config['folder'])
