from threading import Thread
from time import sleep
import webbrowser
from PIL import Image, ImageChops
from pathlib import Path
import imagehash
from send2trash import send2trash
import hashlib
from io import BytesIO
import logging

from .config import config


def open_browser_tab(url):
    def _open_tab():
        sleep(1)
        webbrowser.open_new_tab(url)

    thread = Thread(target=_open_tab)
    thread.daemon = True
    thread.start()


def trim_image(im):
    bg = Image.new(im.mode, im.size, im.getpixel((0,0)))
    diff = ImageChops.difference(im, bg)
    diff = ImageChops.add(diff, diff, 2.0, -100)
    bbox = diff.getbbox()

    if bbox:
        im = im.crop(bbox)

    return im


def shrink_image(im, max_width=800):
    width, height = im.size

    if width > max_width:
        im.thumbnail((max_width, height * max_width / width))

    return im


def remove_duplicate(file_path=config['folder']):
    hashes = set()

    for p in images_in_path(file_path):
        h = get_image_hash(Image.open(p), trim=True)
        if h in hashes:
            print('Deleting {}'.format(p))
            send2trash(p)
        else:
            hashes.add(h)


def remove_non_images(file_path=config['folder']):
    images = set(images_in_path(file_path))
    for p in Path(file_path).glob('**/*.*'):
        if not p.is_dir() and p not in images:
            send2trash(str(p))


def images_in_path(file_path=config['folder']):
    for p in Path(file_path).glob('**/*.*'):
        if not p.is_dir() and not p.name.startswith('.') \
                and p.suffix.lower() in {'.png', '.jpg', '.jp2', '.jpeg', '.gif'}:
            yield p


def complete_path_split(path, relative_to=config['folder']):
    components = []

    path = Path(path)
    if relative_to:
        path = path.relative_to(relative_to)

    while path.name:
        components.append(path.name)

        path = path.parent

    return components


def get_image_hash(im, trim=True, **kwargs):
    if isinstance(im, (str, Path)):
        try:
            im = Image.open(im)
        except OSError:
            return None

    if trim:
        im = trim_image(im)

    return imagehash.whash(
        im,
        hash_size=config['hash_size'],
        **kwargs
    )


def get_checksum(fp):
    if isinstance(fp, BytesIO):
        checksum = hashlib.md5(fp.getvalue()).hexdigest()
    elif isinstance(fp, (str, Path)):
        checksum = hashlib.md5(Path(fp).read_bytes()).hexdigest()
    else:
        logging.error('Cannot generate checksum')
        return None

    return checksum
