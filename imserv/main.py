from threading import Thread
import re
from datetime import datetime, timedelta
from IPython.display import display
import sys
from pathlib import Path
import subprocess
from send2trash import send2trash
import logging
from tqdm import tqdm
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import hashlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from . import db, app
from .util import (open_browser_tab, images_in_path, complete_path_split, remove_non_images,
                   get_image_hash, get_checksum)
from .config import config, IMG_FOLDER_PATH


class FileCreationHandler(FileSystemEventHandler):
    def __init__(self, expected_total):
        self.tqdm = tqdm(
            total=expected_total,
            desc='Number of images created',
            unit='file'
        )

    def on_created(self, event):
        self.tqdm.update()


class ImServ:
    def __init__(self, **kwargs):
        """
        :param kwargs: Accept anything in config dictionary, e.g.
        **{
            'engine': 'postgresql://localhost/imserv',
            'host': 'localhost',
            'port': 8000,
            'debug': False,
            'threaded': False,
            'hash_size': 32,
            'hash_difference_threshold': 0
        }
        """
        config.update(kwargs)

        self.engine = create_engine(config['engine'])
        self.session = sessionmaker(bind=self.engine)()

        config['session'] = self.session

    def __iter__(self):
        return self.search()

    def init(self):
        db.Base.metadata.create_all(self.engine)
        self.refresh()

    def runserver(self):
        def _runserver():
            app.run(
                host=config['host'],
                port=config['port'],
                debug=config['debug']
            )

        def _runserver_in_thread():
            open_browser_tab('http://{}:{}'.format(
                config['host'],
                config['port']
            ))
            self.server_thread = Thread(target=_runserver)
            self.server_thread.daemon = True
            self.server_thread.start()

        if config['threaded'] or 'ipykernel' in ' '.join(sys.argv):
            _runserver_in_thread()
        else:
            _runserver()

    def search(self, filename=None, tags=None, info=None,
               since=None, until=None):
        def _filter_tag(q):
            for db_image in q:
                if any(x in tag for tag in db_image.tags):
                    yield db_image

        def _filter_info(q):
            for db_image in q:
                if v in db_image.info.get(k, ''):
                    yield db_image

        def _filter_filename(q):
            for db_image in q:
                if filename in db_image.filename:
                    yield db_image

        def _filter_accessed_and_sort():
            nonlocal since, until

            q = reversed(sorted(self.session.query(db.Image), key=lambda im: im.modified))

            for db_image in q:
                if since:
                    if isinstance(since, timedelta):
                        since = datetime.now() - since
                    if db_image.modified < since:
                        continue

                if until:
                    if db_image.modified > until:
                        continue

                yield db_image

        query = _filter_accessed_and_sort()

        if tags:
            for x in tags:
                query = _filter_tag(query)

        if info:
            for k, v in info.items():
                query = _filter_info(query)

        if filename:
            query = _filter_filename(query)

        return query

    def search_filename(self, filename_regex):
        for path in images_in_path():
            if re.search(filename_regex, str(path), re.IGNORECASE):
                db_image = self.session.query(db.Image) \
                    .filter_by(filename=str(path.relative_to(IMG_FOLDER_PATH))).first()
                if db_image is None:
                    db_image = db.Image()
                    db_image.path = path

                yield db_image

    def last(self, count=1):
        for i, db_image in enumerate(self.search()):
            if i >= count:
                break
            display(db_image)

    @classmethod
    def import_images(cls, file_path=None, tags=None, skip_hash=True):
        if file_path is None:
            file_path = IMG_FOLDER_PATH

        for p in tqdm(
            tuple(images_in_path(file_path)),
            desc='Number of images imported',
            unit='file'
        ):
            db.Image.from_existing(p, tags=tags, rel_path=p.relative_to(Path(file_path)),
                                   skip_hash=skip_hash)

    def import_pdf(self, pdf_filename, force=False):
        def _extract_pdf():
            filename_hash = hashlib.md5(pdf_filename.encode()).hexdigest()

            number_of_images = len(subprocess.check_output([
                'pdfimages',
                '-list',
                pdf_filename
            ]).split(b'\n')) - 2

            observer = Observer()
            event_handler = FileCreationHandler(expected_total=number_of_images)

            observer.schedule(event_handler, str(dst_folder_path), recursive=False)
            observer.setDaemon(True)
            observer.start()
            observer.join()

            try:
                subprocess.call([
                    'pdfimages',
                    '-p',
                    '-png',
                    pdf_filename,
                    str(dst_folder_path.joinpath(filename_hash))
                ])
            except KeyboardInterrupt:
                pass

            event_handler.tqdm.close()
            observer.stop()

        dst_folder_path = IMG_FOLDER_PATH.joinpath('pdf').joinpath(Path(pdf_filename).stem)

        if not dst_folder_path.exists():
            dst_folder_path.mkdir(parents=True)
            _extract_pdf()
        elif force:
            _extract_pdf()

        self.import_images(file_path=dst_folder_path)

    def get_pdf_image(self, filename_regex, page_start, page_end):
        for db_image in self.search_filename(filename_regex):
            match_obj = re.search(r'(\d+)-\d+\.png', str(db_image.path), flags=re.IGNORECASE)

            if match_obj is not None:
                page_number = int(match_obj.group(1))
                if page_number in range(page_start, page_end+1):
                    yield db_image

    def refresh(self, do_delete=True):
        db_images_path = set()

        for db_image in tqdm(
            tuple(self.session.query(db.Image)),
            desc='Deleting invalid files and update hashes if needed',
            unit='record'
        ):
            if not db_image.path.exists():
                self.session.delete(db_image)
            else:
                checksum = get_checksum(db_image.path)
                if checksum != db_image.checksum:
                    db_image.image_hash = get_image_hash(db_image.path)
                    db_image.checksum = checksum
                    self.session.commit()

                db_images_path.add(db_image.path)

        self.session.commit()

        for file_path in tqdm(
            tuple(images_in_path()),
            desc='Adding new files in PATH',
            unit='file'
        ):
            filename = str(file_path.relative_to(IMG_FOLDER_PATH))

            if file_path not in db_images_path:
                h = get_image_hash(IMG_FOLDER_PATH.joinpath(filename))
                if h is None:
                    continue

                preexisting = self.session.query(db.Image).filter_by(image_hash=h).first()
                if preexisting is None:
                    db_image = db.Image.from_existing(file_path)
                    db_image.add_tags(complete_path_split(file_path.parent))
                else:
                    logging.error('%s conflicts with %s', file_path, preexisting.path)

                    if do_delete:
                        send2trash(str(file_path))

        if do_delete:
            remove_non_images()

    def calculate_hash(self, reset=False):
        if reset:
            for db_image in tqdm(
                self.session.query(db.Image),
                desc='Resetting hashes',
                unit='record'
            ):
                db_image.image_hash = None
            self.session.commit()

        for db_image in tqdm(
            tuple(self.session.query(db.Image).filter_by(image_hash=None)),
            desc='Getting hashes',
            unit='record'
        ):
            if not db_image.path.exists():
                self.session.delete(db_image)
            else:
                h = get_image_hash(db_image.path)
                if h is None:
                    self.session.delete(db_image)
                    self.session.commit()
                    continue

                db_existing = self.session.query(db.Image).filter_by(image_hash=h).first()
                if db_existing is not None:
                    logging.error('Deleting %s (conflicts with %s)', db_image.path, db_existing.path)
                    db_image.delete()
                else:
                    db_image.image_hash = h

                self.session.commit()

        self.refresh()
