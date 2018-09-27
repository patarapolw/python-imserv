from flask import request, jsonify, make_response, abort
from werkzeug.utils import secure_filename

from io import BytesIO

from . import app, db
from .config import config

filename = None


@app.route('/api/images/create', methods=['POST'])
def create_image():
    global filename

    if 'file' in request.files:
        tags = request.form.get('tags')
        file = request.files['file']
        with BytesIO() as bytes_io:
            file.save(bytes_io)
            db_image = db.Image.from_bytes_io(bytes_io,
                                              filename=secure_filename(file.filename), tags=tags)

            if isinstance(db_image, str):
                return abort(make_response(jsonify(message=db_image), 409))
            else:
                filename = db_image.filename

                return jsonify({
                    'filename': db_image.filename,
                    'trueFilename': str(db_image.path)
                }), 201

    response = make_response()
    response.status_code = 304

    return response


@app.route('/api/images/rename', methods=['POST'])
def rename_image():
    global filename

    db_image = config['session'].query(db.Image).filter_by(filename=filename).first()
    if filename is not None and db_image is not None:
        post_json = request.get_json()
        db_image.add_tags(post_json['tags'])
        db_image.move(new_filename=post_json['filename'])

        return jsonify({
            'filename': db_image.filename,
            'trueFilename': str(db_image.path)
        }), 201

    response = make_response()
    response.status_code = 304

    return response
