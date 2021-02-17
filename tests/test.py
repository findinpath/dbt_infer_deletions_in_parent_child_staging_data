import json
import logging
import math
import os
from decimal import Decimal

import dbt
import dbt.main
import dtspec
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.sql import text
import yaml

logging.basicConfig()
logging.getLogger("dbt").setLevel(logging.INFO)

LOG = logging.getLogger('sqlalchemy.engine')
LOG.setLevel(logging.ERROR)

with open(os.path.join(os.getenv('HOME'), '.dbt', 'profiles.yml')) as f:
    DBT_PROFILE = yaml.safe_load(f)['dbt_video_platform']['outputs']['dev']

    SA_ENGINE = sa.create_engine(
        'snowflake://{user}:{password}@{account}.{region}/{database}/{schema}?warehouse={warehouse}&role={role}'.format(
            user=DBT_PROFILE['user'],
            password=DBT_PROFILE['password'],
            account=DBT_PROFILE['account'][:DBT_PROFILE['account'].find('.')],
            region=DBT_PROFILE['account'][DBT_PROFILE['account'].find('.') + 1:],
            database=DBT_PROFILE['database'],
            schema=DBT_PROFILE['schema'],
            warehouse=DBT_PROFILE['warehouse'],
            role=DBT_PROFILE['role']
        )
    )


def init_logger():
    level = logging.DEBUG if os.environ.get('DEBUG', False) else logging.INFO
    logging.getLogger("dbt").setLevel(logging.INFO)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

    logging.basicConfig(level=level, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    console = logging.StreamHandler()
    new_logger = logging.getLogger()
    new_logger.addHandler(console)
    return new_logger


LOGGER = init_logger()


def clean_test_data(api):
    LOGGER.info(
        f"Truncating data from the tables {list(set(list(api.spec['sources'].keys()) + list(api.spec['targets'].keys())))}")
    sqls = [f'TRUNCATE TABLE IF EXISTS {table};' for table in api.spec['targets'].keys()] + \
           [f'TRUNCATE TABLE IF EXISTS {table};' for table in api.spec['sources'].keys()]

    with SA_ENGINE.connect() as conn:
        for sql in set(sqls):
            conn.execute(sql)
        conn.execute("commit")


def load_sources(api):
    metadata = sa.MetaData()

    for source, data in api.spec['sources'].items():

        sa_table = sa.Table(
            source,
            metadata,
            autoload=True,
            autoload_with=SA_ENGINE,
            schema=DBT_PROFILE['schema']
        )

        with SA_ENGINE.connect() as conn:

            # Snowflake has support for VARIANT types
            # https://docs.snowflake.com/en/sql-reference/data-types-semistructured.html
            # The insertion for the VARIANT values is done through `parse_json` function.
            # If on the dtspec data fields inserted there is made use of a VARIANT field, the following
            # logic applies the corresponding conversion of the value (via `parse_json` function).
            data_column_names = list(data.data.columns)
            sa_table_columns = map(lambda data_column_name: next(
                sa_table_column for sa_table_column in sa_table.columns if
                sa_table_column.name == data_column_name),
                                   data_column_names)

            prepared_statement_values = ', '.join(
                ('$' + str(index + 1), 'parse_json($' + str(index + 1) + ')')[
                    str(sa_table_column.type) in ('VARIANT', 'OBJECT')]
                for (index, sa_table_column) in enumerate(sa_table_columns))

            prepared_statement_variables = ', '.join(
                list(map(lambda column_name: ':' + column_name, data_column_names)))

            insert_statement = text(
                                '''
                                insert into {table_name} ({column_names})
                                select {prepared_statement_values}
                                from values (
                                    {prepared_statement_variables}
                                )
                                '''.format(table_name=str(sa_table),
                                           column_names=', '.join(data_column_names),
                                           prepared_statement_values=prepared_statement_values,
                                           prepared_statement_variables=prepared_statement_variables))

            serialized_data = data.serialize()
            if len(serialized_data) == 0:
                LOGGER.warning(f'Load source {sa_table}: no input data')
                return
            LOGGER.info(f'Load source {sa_table}: {len(serialized_data)} records')
            conn.execute(insert_statement, serialized_data)


class DbtRunError(Exception): pass


def run_dbt():
    dbt_project_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
    print(dbt_project_dir)
    dbt_args = ['run', '--project-dir', dbt_project_dir]

    dbt.logger.log_manager.reset_handlers()
    _, success = dbt.main.handle_and_check(dbt_args)
    if not success:
        raise DbtRunError('dbt failed to run successfully, please see log for details')


def _is_nan(value):
    try:
        return math.isnan(value)
    except TypeError:
        return False


def _is_null(value):
    return value in [None, pd.NaT] or _is_nan(value)


def _stringify_sa(df, sa_table):
    for col in sa_table.columns:
        # convert integer values which are represented as float in sqlalchemy back to int
        # see https://stackoverflow.com/a/53434116/497794
        if isinstance(col.type, sa.sql.sqltypes.DECIMAL) and col.type.scale == 0:
            df[col.name] = df[col.name].fillna(0).astype(int).astype(object).where(df[col.name].notnull())
        if pd.api.types.is_numeric_dtype(col):
            df[col.name] = df[col.name].apply(lambda v: "{:.4f}".format((Decimal(v))) if not pd.isna(v) else None)

    nulls_df = df.applymap(_is_null)
    str_df = df.astype({column: str for column in df.columns})

    def _replace_nulls(series1, series2):
        return series1.combine(series2, lambda value1, value2: value1 if not value2 else '{NULL}')

    return str_df.combine(nulls_df, _replace_nulls)


def load_actuals(api):
    metadata = sa.MetaData()
    serialized_actuals = {}
    with SA_ENGINE.connect() as conn:
        for target, _data in api.spec['targets'].items():
            LOGGER.info(f"Loading data from the target table {target}")
            sa_table = sa.Table(
                target,
                metadata,
                autoload=True,
                autoload_with=SA_ENGINE,
                schema=DBT_PROFILE['schema']
            )
            df = _stringify_sa(
                pd.read_sql_table(target, conn, schema=DBT_PROFILE['schema']),
                sa_table
            )

            serialized_actuals[target] = {
                "records": json.loads(df.to_json(orient="records")),
                "columns": list(df.columns),
            }
    api.load_actuals(serialized_actuals)


def test_dtspec():
    for spec_filename in ['tests/demo-spec.yml']:
        LOGGER.info(f"Executing test specification {spec_filename}")
        with open(spec_filename) as f:
            api = dtspec.api.Api(yaml.safe_load(f))

        api.generate_sources()
        clean_test_data(api)
        load_sources(api)
        run_dbt()
        load_actuals(api)
        api.assert_expectations()


if __name__ == '__main__':
    test_dtspec()
