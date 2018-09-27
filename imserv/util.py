from threading import Thread
from time import sleep
import webbrowser
from PIL import Image, ImageChops
from pathlib import Path
import imagehash
from send2trash import send2trash
import os

from .config import IMG_FOLDER_PATH


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


def remove_duplicate(file_path=IMG_FOLDER_PATH):
    hashes = set()

    for p in images_in_path(file_path):
        h = imagehash.dhash(trim_image(shrink_image(Image.open(p))))
        if h in hashes:
            print('Deleting {}'.format(p))
            send2trash(p)
        else:
            hashes.add(h)


def remove_non_images(file_path=IMG_FOLDER_PATH):
    for file_path in images_in_path(file_path):
        send2trash(str(file_path))


def images_in_path(file_path=IMG_FOLDER_PATH):
    for p in Path(file_path).glob('**/*.*'):
        if not p.is_dir() and p.suffix.lower() in {'.png', '.jpg', '.jp2', '.jpeg', '.gif'}:
            yield p


def complete_path_split(path, relative_to=IMG_FOLDER_PATH):
    components = []

    path = Path(path).relative_to(relative_to)
    while path.name:
        components.append(path.name)

        path = path.parent

    return components
