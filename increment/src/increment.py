from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException


def get_last_date(table_name: str, spark_pg_io):
    try:
        last_max_date = (spark_pg_io
            .pg_read_df('dwh.' + table_name)
            .max(F.col('updated_at'))
            .collect()[0][0]
        )
    except AnalysisException:
        # if table does not exist, get min date from source and decrease by 1 day
        min_date = (spark_pg_io
            .pg_read_df('source.' + table_name)
            .select(min("updated_at"))
            .first()[0])
        one_day_ago = F.date_sub(min_date, 1)
        last_max_date = F.date_format(one_day_ago, "yyyy-MM-dd")
    
    return last_max_date


def increment_table(table_name: str, spark_pg_io, log) -> None:
    curr_date = F.current_timestamp()
    
    last_max_date = get_last_date(table_name, spark_pg_io)
    
    log.info(f'increment for table dwh.{table_name}, {last_max_date}-{curr_date}')
    
    all_data: DataFrame = spark_pg_io.pg_read_df('source.' + table_name)
    new_data = (all_data
        .filter(F.col('updated_at') > last_max_date)
        .withColumn('load_dttm', F.lit(curr_date))
    )
    
    return new_data