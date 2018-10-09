import sys
import re
from threading import Thread
import subprocess
from tqdm import tqdm
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import random

from .config import config
from .util import open_browser_tab, images_in_path, get_checksum, get_image_hash
from . import app

__all__ = ('ImServ',)


class FileCreationHandler(FileSystemEventHandler):
    def __init__(self, expected_total):
        self.tqdm = tqdm(
            total=expected_total
        )

    def on_created(self, event):
        self.tqdm.update()


class ImServ:
    def __init__(self, **kwargs):
        config.update(kwargs)

        from . import db

        self.db = dict(
            image=db.Image,
            note=db.Note,
            tag=db.Tag
        )

    def __getitem__(self, item):
        return self.db[item]

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

    def search_filename(self, filename_regex, calculate_hash=False):
        def _search():
            for file_path in tqdm(tuple(images_in_path())):
                if re.search(filename_regex, str(file_path), flags=re.IGNORECASE):
                    db_image = self._get_or_create(file_path, calculate_hash)

                    yield db_image

        return sorted(_search())

    def search_database(self, query):
        return self.db['image'].select().where(query)

    def refresh(self, calculate_hash=False):
        for file_path in tqdm(tuple(images_in_path())):
            db_image = self._get_or_create(file_path, calculate_hash)
            if db_image is not None:
                if calculate_hash:
                    db_image.image_hash = get_image_hash(file_path)
                    db_image.save()

    def _get_or_create(self, file_path, calculate_hash):
        checksum = get_checksum(file_path)
        image_hash = None
        if calculate_hash:
            image_hash = get_image_hash(file_path)

        db_image = self.db['image'].get_or_none(
            file_id=file_path.stat().st_ino
        )
        if db_image is None:
            db_image = self.db['image'].create(
                file_id=file_path.stat().st_ino,
                checksum=checksum,
                image_hash=image_hash,
                filename=str(file_path)
            )
        else:
            if image_hash:
                db_image.image_hash = image_hash

            if checksum != db_image.checksum:
                db_image.checksum = checksum

            db_image.filename = str(file_path)
            db_image.save()

        return db_image

    def import_pdf(self, pdf_filename, calculate_hash=False):
        """
        Import images from a PDF. Poppler (https://poppler.freedesktop.org) will be required.
        In Mac OSX, `brew install poppler`.
        In Linux, `yum install poppler-utils` or `apt-get install poppler-utils`.

        Arguments:
            pdf_filename {str, pathlib.Path} -- Path to PDF file.
        """

        def _extract_pdf():
            filename_initials = ''.join(c for c in str(Path(pdf_filename).name) if c.isupper())
            if not filename_initials:
                filename_initials = pdf_filename[0]

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

            try:
                subprocess.call([
                    'pdfimages',
                    '-p',
                    '-png',
                    str(pdf_filename),
                    str(dst_folder_path.joinpath(filename_initials))
                ])
                observer.stop()
            except KeyboardInterrupt:
                observer.stop()

            observer.join()

            event_handler.tqdm.close()

        dst_folder_path = config['folder'].joinpath('pdf').joinpath(Path(pdf_filename).stem)

        if not dst_folder_path.exists():
            dst_folder_path.mkdir(parents=True)

        _extract_pdf()

        for file_path in tqdm(tuple(images_in_path(dst_folder_path))):
            self._get_or_create(file_path, calculate_hash=calculate_hash)

    def get_pdf_image(self, filename_regex, page_start, page_end,
                      calculate_hash=True, randomize=False):
        """Search images corresponding to PDF in config['folder']

        Arguments:
            filename_regex {str} -- Regex matching the PDF filename
            page_start {int} -- First page to search
            page_end {int} -- Last page to search
        Yields:
            db.Image object corresponding to the criteria
        """

        def _get_image():
            for file_path in images_in_path():
                match_obj = re.search(rf'{filename_regex}.*(?:[^\d])(\d+)-\d+\.png', str(file_path), flags=re.IGNORECASE)

                if match_obj is not None:
                    page_number = int(match_obj.group(1))
                    if page_number in range(page_start, page_end + 1):
                        db_image = self._get_or_create(file_path, calculate_hash)

                        yield page_number, db_image

        if randomize:
            images = [db_image for i, db_image in _get_image()]
            random.shuffle(images)

            return images
        else:
            return [db_image for i, db_image in sorted(_get_image(), key=lambda x: x[0])]

    def last_created(self):
        return self.db['image'].select().order_by(self.db['image'].id.desc())
