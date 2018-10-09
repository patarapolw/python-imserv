from peewee import PostgresqlDatabase
from playhouse.migrate import PostgresqlMigrator, migrate


if __name__ == '__main__':
    migrator = PostgresqlMigrator(PostgresqlDatabase('imserv'))
    migrate(
        migrator.drop_column('image', 'created')
    )
