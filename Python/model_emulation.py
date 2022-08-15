import time
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import Table, Column, Numeric, DateTime
from sqlalchemy.dialects.postgresql import insert as pg_insert


class DB:
    def __init__(self, config):
        self.config = config['db_settings']
        self.table = self.define_table()
        self.engine = self.create_engine()

    def create_engine(self):
        conn_string = self.config['dialect'] + '://' + \
                      self.config['username'] + ':' + \
                      self.config['password'] + '@' + \
                      self.config['host'] + ':' + \
                      self.config['port'] + '/' + \
                      self.config['db_name']
        return sa.create_engine(conn_string)

    def define_table(self):
        metadata_obj = sa.MetaData()
        return Table(self.config['table'], metadata_obj,
                     Column('datetime', DateTime, nullable=False, primary_key=True),
                     Column('loading', Numeric),
                     Column('pressure', Numeric),
                     Column('temperature', Numeric),
                     Column('quality_true', Numeric),
                     Column('quality_predict', Numeric),
                     schema=self.config['schema']
                     )

    def drop_table(self):
        self.table.drop(self.engine, checkfirst=True)

    def create_table(self):
        self.table.create(self.engine, checkfirst=True)

    def recreate_table(self):
        self.drop_table()
        self.create_table()

    def insert_data(self, data: pd.Series):
        # Update on conflict postgres insert
        with self.engine.connect() as conn:
            insert_stmt = pg_insert(self.table).values(data.to_dict())
            do_update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['datetime'],
                set_=data.to_dict()
            )
            conn.execute(do_update_stmt)


def main(config: dict):
    db = DB(config)
    # Обновляем таблицу в БД
    db.recreate_table()

    df = pd.read_csv(config['data_path'])

    # Итерация по строкам датасета
    for i, (_, row) in enumerate(df.iterrows()):
        # Записываем "текущие" показания (все колонки кроме datetime_predict, quality_predict, dt_diff)
        db.insert_data(row[:-3])

        # Записываем в БД прогноз на 30 минут вперед
        predict = row[['datetime_predict', 'quality_predict']]
        predict.rename({'datetime_predict': 'datetime'}, axis=1, inplace=True)
        db.insert_data(predict)

        print(f'{i}: Data with timestamp {row["datetime"]} and predict to {predict["datetime"]} were sent')
        # "Спим" только если записали больше строк, чем указано в параметре "preload_rows"
        if i > config['preload_rows'] - 1:
            time.sleep(row['dt_diff'] / config['speedx'])


if __name__ == '__main__':
    config = {'db_settings': {'dialect': 'postgresql',
                              'host': '127.0.0.1',
                              'port': '5432',
                              'db_name': 'postgres',
                              'username': 'postgres',
                              'password': 'qazwsx12345',  # ваш пароль от пользователя "Postgres"
                              'schema': 'public',
                              'table': 'fakedata',  # произвольное название таблицы
                              },
              'data_path': 'data/df_emulation.csv',  # путь к подготовленным данным
              'preload_rows': 500,  # количество строк для предварительной загрузки
              'speedx': 120  # во сколько раз ускорить загрузку
              }
    main(config)
