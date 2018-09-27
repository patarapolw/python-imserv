from flask import render_template, send_file, request
from urllib.parse import unquote
from pathlib import Path

from .config import IMG_FOLDER_PATH

from . import app


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/images')
def get_image():
    file_path = Path(unquote(request.args.get('filename')))
    if not file_path.exists():
        file_path = IMG_FOLDER_PATH.joinpath(file_path)
    print('Serving: {}'.format(file_path))

    return send_file(str(file_path))
