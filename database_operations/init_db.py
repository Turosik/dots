from logging import INFO

import sqlalchemy as sa
from sqlalchemy.engine import reflection
from sqlalchemy.ext.declarative import declarative_base

import database_operations.db as db_module
from database_operations.utils import create_engine_sync

Base = declarative_base()


def add_column_if_not_exists(engine, table, column_name):
    """Add a column to a table if the column does not already exist."""
    with engine.begin() as conn:
        inspector = sa.inspect(engine)
        columns = inspector.get_columns(table.name)
        column_names = [col['name'] for col in columns]

        if column_name not in column_names:
            column = table.columns[column_name]
            col_type = column.type.compile(engine.dialect)
            column_definition = f"{column_name} {col_type}"

            if not column.nullable:
                column_definition += " NOT NULL"
            if column.server_default is not None:
                default_value = column.server_default.arg
                column_definition += f" DEFAULT {default_value}"

            alter_stmt = f"ALTER TABLE {table.name} ADD COLUMN {column_definition};"
            conn.execute(sa.text(alter_stmt))


def check_db_structure(config, symbols, log):
    engine = create_engine_sync(config, echo=False)
    inspector = reflection.Inspector.from_engine(engine)

    all_tables = []
    # Checking the existence of each table
    for cls in [db_module.Symbols, db_module.Dots]:
        all_tables.append(cls.__table__)
        table_name = cls.__tablename__
        log(INFO, f'Checking table {table_name}')

        # Inspect if the table exists
        if not inspector.has_table(table_name):
            Base.metadata.create_all(bind=engine, tables=[cls.__table__])
            log(INFO, f'Table {table_name} created')
        else:
            log(INFO, f'Table {table_name} exists')

    # Loop through each table and its columns
    for table in all_tables:
        for column_name in table.columns.keys():
            log(INFO, f'Checking table {table} column {column_name}')
            add_column_if_not_exists(engine, table, column_name)

    # Now checking symbols
    for symbol in symbols:
        log(INFO, f'Checking symbol {symbol}')
        db_module.symbols_add_if_not_exists_sync(engine, symbol)

    engine.dispose()
    del engine
