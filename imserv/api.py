from flask import request, jsonify, make_response, abort
from werkzeug.utils import secure_filename
from pathlib import Path

from io import BytesIO

from . import app, db


@app.route('/api/images/create', methods=['POST'])
def create_image():
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
                return jsonify({
                    'filename': str(Path(db_image.filename).name),
                    'trueFilename': str(db_image.filename)
                }), 201

    response = make_response()
    response.status_code = 304

    return response
