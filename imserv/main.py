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
from .config import config


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
            'hash_difference_threshold': 0,
            'folder': IMG_FOLDER_PATH
        }
        """
        config.update(kwargs)

        self.engine = create_engine(config['engine'])
        self.session = sessionmaker(bind=self.engine)()

        config['session'] = self.session

    def __iter__(self):
        """
        
        Returns:
            Iterator of all db.Image's in the database
        """

        return self.search()

    def init(self):
        """Initiate the database for the first time (see README.md)
        """

        db.Base.metadata.create_all(self.engine)
        self.refresh()

    def runserver(self):
        """Run the image server (see README.md)
        """

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
        """Search the image database
        
        Keyword Arguments:
            filename {str} -- Substring of the filename to query (default: {None})
            tags {iterable} -- Iterable of substrings of tags (default: {None})
            info {str} -- Substring of the db.Image.info_json (default: {None})
            since {datetime.datetime} -- Start datetime of db.Image.modified (default: {None})
            until {datetime.datetime} -- End datetime of db.Image.modified (default: {None})
        
        Returns:
            iterator -- Iterator of db.Image
        """

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
        """Search filename in the config['ima'] directly (without needing to be embedded in the database.)
        
        Arguments:
            filename_regex {str} -- regex matching the filename
        """

        for path in images_in_path():
            if re.search(filename_regex, str(path), re.IGNORECASE):
                db_image = self.session.query(db.Image) \
                    .filter_by(filename=str(path.relative_to(config['folder']))).first()
                if db_image is None:
                    db_image = db.Image()
                    db_image.path = path

                yield db_image

    def last(self, count=1):
        """View the latest added db.Image's
        
        Keyword Arguments:
            count {int} -- Number of db.Image's to view (default: {1})
        """

        for i, db_image in enumerate(self.search()):
            if i >= count:
                break
            display(db_image)

    @classmethod
    def import_images(cls, file_path=None, tags=None, skip_hash=False):
        """Import images from a file path
        
        Keyword Arguments:
            file_path {str, pathlib.Path} -- 
                File/folder path to scan. By default, config['folder'] will be scanned. (default: {None})
            tags {iterable} -- Iterable of substring of tags (default: {None})
            skip_hash {bool} -- If duplication prevention (hashing) is slow, switch this to True (default: {True})
        """

        if file_path is None:
            file_path = config['folder']

        for p in tqdm(
            tuple(images_in_path(file_path)),
            desc='Number of images imported',
            unit='file'
        ):
            db.Image.from_existing(p, tags=tags, rel_path=p.relative_to(Path(file_path)),
                                   skip_hash=skip_hash)

    def import_pdf(self, pdf_filename):
        """
        Import images from a PDF. Poppler (https://poppler.freedesktop.org) will be required.
        In Mac OSX, `brew install poppler`.
        In Linux, `yum install poppler-utils` or `apt-get install poppler-utils`.
        
        Arguments:
            pdf_filename {str, pathlib.Path} -- Path to PDF file.
        """

        def _extract_pdf():
            filename_hash = hashlib.md5(pdf_filename.encode()).hexdigest()

            number_of_images = len(subprocess.check_output([
                'pdfimages',
                '-list',
                str(pdf_filename)
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
                    str(pdf_filename),
                    str(dst_folder_path.joinpath(filename_hash))
                ])
            except KeyboardInterrupt:
                pass

            event_handler.tqdm.close()
            observer.stop()

        dst_folder_path = config['folder'].joinpath('pdf').joinpath(Path(pdf_filename).stem)

        if not dst_folder_path.exists():
            dst_folder_path.mkdir(parents=True)
            _extract_pdf()

        self.import_images(file_path=dst_folder_path)

    def get_pdf_image(self, filename_regex, page_start, page_end):
        """Search images corresponding to PDF in config['folder']
        
        Arguments:
            filename_regex {str} -- Regex matching the PDF filename
            page_start {int} -- First page to search
            page_end {int} -- Last page to search
        Yields:
            db.Image object corresponding to the criteria
        """


        for db_image in self.search_filename(filename_regex):
            match_obj = re.search(r'(\d+)-\d+\.png', str(db_image.path), flags=re.IGNORECASE)

            if match_obj is not None:
                page_number = int(match_obj.group(1))
                if page_number in range(page_start, page_end+1):
                    yield db_image

    def refresh(self, do_delete=True):
        """Refresh the image database.
        
        Keyword Arguments:
            do_delete {bool} -- 
                Delete the invalid files in config['folder'] if possible (by sending to trash). (default: {True})
        """

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
            filename = str(file_path.relative_to(config['folder']))

            if file_path not in db_images_path:
                h = get_image_hash(config['folder'].joinpath(filename))
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
        """Calculate hashes for images in the database
        
        Keyword Arguments:
            reset {bool} -- If true, all images will be recalculated the hash (default: {False})
        """

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
