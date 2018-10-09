import peewee as pv

import imagehash
from uuid import uuid4
from nonrepeat import nonrepeat_filename
from slugify import slugify
import PIL.Image
import os
import sys
from datetime import datetime
from urllib.parse import quote, urlparse

from .config import config
from .util import get_checksum, get_image_hash

__all__ = ('Tag', 'Image', 'ImageTags', 'create_all_tables')


class BaseModel(pv.Model):
    class Meta:
        database = pv.PostgresqlDatabase(config['database'])


class ImageHashField(pv.TextField):
    def db_value(self, value):
        if value:
            return str(value)

    def python_value(self, value):
        if value:
            return imagehash.hex_to_hash(value)


class Tag(BaseModel):
    name = pv.TextField()

    def to_json(self):
        return {
            'name': self.name,
            'images': [img.to_json() for img in self.images]
        }


class Image(BaseModel):
    file_id = pv.IntegerField(null=True)
    filename = pv.TextField(null=False)
    checksum = pv.TextField(null=False)
    image_hash = ImageHashField(null=True)
    tags = pv.ManyToManyField(Tag, backref='images')

    @classmethod
    def from_bytes_io(cls, im_bytes_io, filename=None, tags=None):
        """
        :param im_bytes_io:
        :param str filename:
        :param str|list|tuple tags:
        :return:
        """

        if tags is None:
            tags = list()

        if not filename or filename == 'image.png':
            filename = str(uuid4())[:8] + '.png'

        filename = nonrepeat_filename(filename,
                                      primary_suffix=slugify('-'.join(tags)),
                                      root=str(config['blob_folder']))

        filename = str(config['blob_folder'].joinpath(filename))
        checksum = get_checksum(im_bytes_io)

        im = PIL.Image.open(im_bytes_io)
        image_hash = get_image_hash(im)

        im.save(filename)

        db_image = cls.create(
            file_id=os.stat(filename).st_ino,
            filename=filename,
            checksum=checksum,
            image_hash=image_hash
        )

        for tag in tags:
            db_image.tags.add(Tag.get_or_create(name=tag)[0])

        return db_image

    @classmethod
    def from_existing(cls, filename, tags=None):
        if tags is None:
            tags = list()

        filename = str(filename)

        db_image = cls.create(
            file_id=os.stat(filename).st_ino,
            filename=filename,
            checksum=get_checksum(filename),
            image_hash=get_image_hash(filename)
        )

        for tag in tags:
            db_image.tags.add(Tag.get_or_create(name=tag)[0])

        db_image.path = filename

        return db_image

    @classmethod
    def similar_images(cls, im):
        image_hash = get_image_hash(im)

        if config['hash_difference_threshold']:
            for db_image in cls.select():
                if db_image.image_hash - image_hash < config['hash_difference_threshold']:
                    yield db_image
        else:
            return cls.select().where(cls.image_hash == image_hash)

    def get_image(self, max_width=800, max_height=800):
        url = self.url
        if url:
            return f'<img src="{url}" style="max-width: {max_width}px; max-height: {max_height}px;" />'

    def _repr_html_(self):
        return self.get_image(800, 800)

    def _repr_json_(self):
        result = self.to_json()
        result['image'] = self.filename

        return result

    @property
    def url(self):
        if not urlparse(self.filename).netloc:
            return 'http://{}:{}/images?filename={}'.format(
                config['host'],
                config['port'],
                quote(str(self.filename), safe='')
            )
        else:
            return self.filename

    def to_json(self):
        return dict(
            id=self.id,
            image=self.get_image(400, 400),
            filename=self.filename,
            notes=[n.data for n in self.notes],
            tags=[t.name for t in self.tags]
        )

    handsontable_config = {
        'renderers': {
            'image': 'html'
        },
        'config': {
            'colWidths': {
                'image': 400
            }
        }
    }


ImageTags = Image.tags.get_through_model()


class Note(BaseModel):
    image = pv.ForeignKeyField(Image, backref='notes')
    data = pv.TextField()

    def _repr_markdown_(self):
        return self.data

    def to_json(self):
        return {
            'file_id': self.file_.to_json(),
            'data': self.data
        }


def create_all_tables():
    for cls in sys.modules[__name__].__dict__.values():
        if hasattr(cls, '__bases__') and issubclass(cls, pv.Model):
            if cls is not BaseModel:
                cls.create_table()
