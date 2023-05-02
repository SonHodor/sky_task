import logging

from pyspark.sql import SparkSession

from app_config import AppConfig
from src.increment import increment_table


"""
    Не понятны условия задачи. Написал так, что инремент берётся по updated_at
    в источнике и по созданному мной полю с датой записи в таблице dwh.
    
    Но тут также можно понять задачу, как поддерживать данные в состоянии,
    указанном в источнике, то есть удалять/изменять/создавать строки.
    
    Так же можно предположить, что ETL процесс будет запускаться раз в неделю,
    и все действия должны быть "свёрнуты", то есть если идёт создание-изменение-
    удаление, то всё это можно свернуть в то, что запись в принципе не надо 
    создавать в dwh. 
    Всего этого не было указано.
"""


def main():
    log = logging.getLogger()
    log.basicConfig(
        level=logging.INFO,
        format="INCREMENT \t%(message)s"
        )
    config = AppConfig()
    
    log.info(f'Start increment load of tables {" ".join(config.tables)}')
    
    spark_pg_io = config.spark_pg_io()
    
    for table_name in config.tables:
        inc_data = increment_table(table_name, spark_pg_io, log)
        spark_pg_io.pg_write(inc_data, 'dwh.' + table_name, 'append')
        log.info(f'done writing dwh.{table_name}')


if __name__ == '__main__':
    main()