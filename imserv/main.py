from threading import Thread
import re
from datetime import datetime, timedelta
from IPython.display import display
import sys
import os
from pathlib import Path
import subprocess
import imagehash
import PIL.Image
from send2trash import send2trash
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from . import db, app
from .util import open_browser_tab, images_in_path, complete_path_split, remove_non_images
from .config import config, IMG_FOLDER_PATH


class ImServ:
    def __init__(self, port='8000', debug=False, **kwargs):
        """

        :param port:
        :param debug:
        :param kwargs: Accept anything in config dictionary, e.g. similarity_threshold (default=3)
        """
        os.environ.update({
            'HOST': 'localhost',
            'PORT': port,
            'DEBUG': '1' if debug else '0'
        })

        self.engine = create_engine('postgresql://localhost/imserv')
        self.session = sessionmaker(bind=self.engine)()

        config.update({
            'session': self.session,
            **kwargs
        })

    def runserver(self):
        def _runserver():
            app.run(
                host=os.getenv('HOST', 'localhost'),
                port=os.getenv('PORT', '8000'),
                debug=True if os.getenv('DEBUG', '0') == '1' else False
            )

        def _runserver_in_thread():
            open_browser_tab('http://{}:{}'.format(
                os.getenv('HOST', 'localhost'),
                os.getenv('PORT', '8000')
            ))
            self.server_thread = Thread(target=_runserver)
            self.server_thread.daemon = True
            self.server_thread.start()

        if 'ipykernel' in ' '.join(sys.argv):
            _runserver_in_thread()
        elif os.getenv('THREADED_IMAGE_SERVER', '0') == '1':
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

        query = self.session.query(db.Image)
        if since:
            if isinstance(since, timedelta):
                since = datetime.now() - since
            query = query.filter(db.Image.modified > since)
        if until:
            query = query.filter(db.Image.modified < until)

        query = iter(query.order_by(db.Image.modified.desc()))

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
    def import_images(cls, file_path=None, tags=None):
        if file_path is None:
            file_path = IMG_FOLDER_PATH

        for p in images_in_path(file_path):
            db.Image.from_existing(p, tags=tags)

    def import_pdf(self, pdf_filename, force=False):
        def _extract_pdf():
            subprocess.call([
                'pdfimages',
                '-p',
                '-png',
                pdf_filename,
                str(dst_folder_path.joinpath('img'))
            ])

        dst_folder_path = IMG_FOLDER_PATH.joinpath('pdf').joinpath(Path(pdf_filename).stem)

        if not dst_folder_path.exists():
            dst_folder_path.mkdir(parents=True)
            _extract_pdf()
        elif force:
            _extract_pdf()

        self.import_images(file_path=dst_folder_path,
                           tags=pdf_filename)

    def get_pdf_image(self, filename_regex, page_start, page_end):
        for db_image in self.search_filename(filename_regex):
            match_obj = re.search(r'(\d+)-\d+\.png', str(db_image.path), flags=re.IGNORECASE)

            if match_obj is not None:
                page_number = int(match_obj.group(1))
                if page_number in range(page_start, page_end+1):
                    yield db_image

    def refresh(self, do_delete=True):
        db_images_path = set()

        for db_image in self.session.query(db.Image):
            if not db_image.path.exists():
                self.session.delete(db_image)
            else:
                db_images_path.add(db_image.path)

        self.session.commit()

        for file_path in images_in_path():
            filename = str(file_path.relative_to(IMG_FOLDER_PATH))

            if file_path not in db_images_path:
                h = str(imagehash.dhash(PIL.Image.open(IMG_FOLDER_PATH.joinpath(filename))))
                if self.session.query(db.Image).filter_by(image_hash=h).first() is None:
                    db_image = db.Image()
                    db_image.filename = filename
                    db_image.image_hash = h

                    self.session.add(db_image)
                    self.session.commit()

                    db_image.add_tags(complete_path_split(file_path.parent))
                else:
                    logging.error('%s already exists.', file_path)

                    if do_delete:
                        send2trash(str(file_path))

        if do_delete:
            remove_non_images()
