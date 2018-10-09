import click

from .main import ImServ


@click.command()
@click.option('--port', default=29635)
def runserver(port):
    ImServ(port=port).runserver()


if __name__ == '__main__':
    runserver()
