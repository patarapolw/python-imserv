# ImServ

Spin an image server, store images from Clipboard in single place, and prevent duplication.

## Installation

- Clone the project from GitHub
- Navigate to the project folder and [`poetry install`](https://github.com/sdispater/poetry)
- Run a Python script:

```python
from imserv import ImServ
ImServ().init()
```
The folder `~/Pictures/imserv` will be created, and will be used for storing all images. (Tested only on Mac.)

By default, it uses PostGRESQL of the database named `imserv`, so you have to initialize the database first, before running the script, but you can also use SQLite by specifying:

```python
ImServ(engine='sqlite:///PATH_TO_SQLITE_DATABASE')
```
## Usage

To run an image server (before running Jupyter Notebook), run in a script:

```python
ImServ().runserver()
```

This will open the server for the website: http://localhost:8000/, which leads to:

![web.png](/screenshots/web.png)

where you can create an image directly from Clipboard. The name of the image will be randomized using UUID.

After that, if you want to use it in Jupyter Notebook:

```python
>>> from imserv import ImServ
>>> ims = ImServ()
>>> ims.last()
'The image accessed last will be shown.'
```

## More screenshots

![jupyter0.png](/screenshots/jupyter0.png)
![jupyter1.png](/screenshots/jupyter1.png)

## Related projects

- [jupyter-flashcard](https://github.com/patarapolw/jupyter-flashcard) - Create a database of Jupyter Notebooks and convert them into flashcards.
