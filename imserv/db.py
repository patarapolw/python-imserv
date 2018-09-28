from datetime import datetime
from pathlib import Path
import shutil
from nonrepeat import nonrepeat_filename
import imagehash
from uuid import uuid4
from slugify import slugify
import logging
import PIL.Image
from urllib.parse import quote
from send2trash import send2trash
import json
import os
import time

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

from .util import (complete_path_split, trim_image, shrink_image,
                   get_image_hash, get_checksum)
from .config import config, IMG_FOLDER_PATH

Base = declarative_base()


class Image(Base):
    __tablename__ = 'image'

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String, nullable=False, unique=True)
    checksum = Column(String, nullable=False)
    image_hash = Column(String, unique=True)

    info_json = Column(String, nullable=True)
    tags_str = Column(String, nullable=True)

    def to_json(self):
        return {
            'id': self.id,
            'filename': self.filename,
            'modified': datetime.fromtimestamp(self.modified).isoformat(),
            'info': self.info,
            'tags': self.tags
        }

    @property
    def modified(self):
        return self.path.stat().st_atime

    def update_modified(self, value=None):
        if value:
            mtime = time.mktime(value.timetuple())
        else:
            mtime = time.time()

        os.utime(str(self.path), (mtime, mtime))

    @property
    def url(self):
        return 'http://{}:{}/images?filename={}'.format(
            config['host'],
            config['port'],
            quote(str(self.path), safe='')
        )

    def to_url(self):
        return '<img src="{}" style="max-width: 800px;" />'.format(self.url)

    def _repr_html_(self):
        return self.to_url()

    def move(self, new_filename):
        new_filename = Path(new_filename).relative_to(IMG_FOLDER_PATH)
        new_filename = new_filename \
            .with_name(new_filename.name) \
            .with_suffix(self.path.suffix)

        if self.filename and self.filename != new_filename:
            new_filename = nonrepeat_filename(str(new_filename),
                                              primary_suffix='-'.join(self.tags),
                                              root=str(IMG_FOLDER_PATH))

            true_filename = IMG_FOLDER_PATH.joinpath(new_filename)
            true_filename.parent.mkdir(parents=True, exist_ok=True)

            shutil.move(str(self.path), str(true_filename))

            self.filename = new_filename
            config['session'].commit()

            return new_filename

    @property
    def tags(self):
        if self.tags_str:
            return self.tags_str.split('\n')
        else:
            return list()

    def add_tags(self, tags):
        if isinstance(tags, str):
            tags = [tags]

        if self.tags:
            self.tags_str = '\n'.join(set(self.tags) | set(tags))
        else:
            self.tags_str = '\n'.join(set(tags))

        config['session'].commit()

        return self.tags

    def remove_tags(self, tags):
        if isinstance(tags, str):
            tags = [tags]

        if self.tags:
            self.tags_str = '\n'.join(set(self.tags) - set(tags))

        config['session'].commit()

        return self.tags

    @property
    def info(self):
        if self.info_json:
            return json.loads(self.info_json)
        else:
            return dict()

    def add_info(self, **kwargs):
        info_dict = self.info
        info_dict.update(kwargs)
        self.info_json = json.dumps(info_dict)

        config['session'].commit()

        return info_dict

    def remove_info(self, key):
        info_dict = self.info
        info_dict.pop(key)
        self.info_json = json.dumps(info_dict)

        config['session'].commit()

        return info_dict

    @classmethod
    def from_bytes_io(cls, im_bytes_io, filename=None, tags=None):
        """

        :param im_bytes_io:
        :param str filename:
        :param str|list|tuple tags:
        :return:
        """
        if not filename or filename == 'image.png':
            filename = 'blob/' + str(uuid4())[:8] + '.png'

        IMG_FOLDER_PATH.joinpath(filename).parent.mkdir(parents=True, exist_ok=True)

        filename = str(IMG_FOLDER_PATH.joinpath(filename)
                       .relative_to(IMG_FOLDER_PATH))
        filename = nonrepeat_filename(filename,
                                      primary_suffix=slugify('-'.join(tags)),
                                      root=str(IMG_FOLDER_PATH))

        return cls._create(filename, tags=tags, pil_handle=im_bytes_io)

    @classmethod
    def from_existing(cls, abs_path, rel_path=None, tags=None, skip_hash=False):
        """

        :param str|Path abs_path:
        :param str|Path rel_path:
        :param list tags:
        :param bool skip_hash:
        :return:
        """
        if tags is None:
            tags = list()

        abs_path = Path(abs_path)
        is_relative = False

        if rel_path is None:
            try:
                rel_path = abs_path.relative_to(IMG_FOLDER_PATH)
                is_relative = True
            except ValueError:
                rel_path = Path(abs_path.name)

        tags.extend(complete_path_split(rel_path.parent, relative_to=None))

        db_image = cls._create(filename=str(rel_path), tags=tags, pil_handle=abs_path,
                               trim=False, shrink=False)
        if isinstance(db_image, str):
            if is_relative:
                send2trash(str(abs_path))

        return db_image

    @classmethod
    def _create(cls, filename, tags, pil_handle,
                trim=True, shrink=True):
        def _process_image():
            try:
                _im = PIL.Image.open(pil_handle)
            except OSError:
                return None

            if trim:
                _im = trim_image(_im)
            if shrink:
                _im = shrink_image(_im)

            return _im

        filename = str(filename)
        IMG_FOLDER_PATH.joinpath(filename).parent.mkdir(parents=True, exist_ok=True)

        true_filename = IMG_FOLDER_PATH.joinpath(filename)
        do_save = True
        if true_filename.exists():
            do_save = False
            checksum = get_checksum(true_filename)
        else:
            checksum = get_checksum(pil_handle)
            if checksum is None:
                return

        db_image = None
        if checksum:
            db_image = config['session'].query(cls).filter_by(checksum=checksum).first()
            if db_image is not None:
                if db_image.filename != filename:
                    db_image.update_modified()

                    err_msg = 'Similar image exists: {}'.format(db_image.path)
                    logging.error(err_msg)
                    return err_msg
                else:
                    return db_image

        if db_image is None:
            im = None
            h = None

            if config['skip_hash']:
                if trim or shrink:
                    im = _process_image()
            else:
                im = _process_image()
                if im is not None:
                    h = get_image_hash(im)
                    if h is None:
                        err_msg = 'Cannot read file {}'.format(filename)
                        logging.error(err_msg)
                        return err_msg

                    for pre_existing in cls.similar_images_by_hash(h):
                        if pre_existing.filename != filename:
                            pre_existing.update_modified()

                            err_msg = 'Similar image exists: {}'.format(pre_existing.path)
                            logging.error(err_msg)
                            return err_msg
                        else:
                            db_image = pre_existing
                else:
                    err_msg = 'Cannot read file {}'.format(filename)
                    logging.error(err_msg)
                    return err_msg

            if do_save:
                if isinstance(pil_handle, (str, Path)):
                    shutil.copy(str(pil_handle), true_filename)
                else:
                    im = _process_image()

                if im:
                    im.save(true_filename)

            if db_image is None:
                db_image = cls()
                db_image.filename = filename
                db_image.checksum = checksum
                db_image.image_hash = h
                config['session'].add(db_image)
                config['session'].commit()

                if tags:
                    db_image.add_tags(tags)

                db_image.update_modified()

        return db_image

    @classmethod
    def add(cls, fp, tags=None, **kwargs):
        if isinstance(fp, (str, Path)):
            return cls.from_existing(fp, tags=tags, **kwargs)
        else:
            return cls.from_bytes_io(fp, tags=tags, **kwargs)

    def delete(self):
        if self.exists():
            send2trash(str(self.path))

        config['session'].delete(self)
        config['session'].commit()

    def exists(self):
        return self.path.exists()

    @property
    def path(self):
        return IMG_FOLDER_PATH.joinpath(self.filename)

    @path.setter
    def path(self, file_path):
        self.filename = str(file_path.relative_to(IMG_FOLDER_PATH))

    @classmethod
    def similar_images_by_hash(cls, h):
        if config['hash_difference_threshold']:
            for db_image in config['session'].query(cls).all():
                if imagehash.hex_to_hash(db_image.image_hash) - imagehash.hex_to_hash(h) \
                        < config['hash_difference_threshold']:
                    yield db_image
        else:
            return iter(config['session'].query(cls).filter_by(image_hash=h))

    @classmethod
    def similar_images(cls, im):
        h = get_image_hash(im)
        if h is None:
            return

        yield from cls.similar_images_by_hash(h)
