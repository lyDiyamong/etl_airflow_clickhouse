import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from .base import DataSource, DataTransformer, DataLoader

logger = logging.getLogger(__name__)

class ETLPipeline:
    """ETL Pipeline orchestrator"""
    def __init__(self, extractor: DataSource, transformer: DataTransformer, loader: DataLoader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        try:
            # Extract
            data = self.extractor.extract()
            logger.info("Data extracted successfully")

            # Transform
            transformed_data = self.transformer.transform(data)
            logger.info("Data transformed successfully")

            # Load
            self.loader.load(transformed_data)
            logger.info("ETL pipeline completed successfully")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            raise
        finally:
            self.extractor.close()


def create_dag(
    dag_id,
    schedule,
    extractor: DataSource,
    transformer: DataTransformer,
    loader: DataLoader,
    default_args=None,
    description=None,
    tags=None
):
    """DAG factory function"""
    if default_args is None:
        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 1,
        }

    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        start_date=days_ago(1),
        catchup=False,
        description=description,
        tags=tags or [],
    )

    def run_etl(**context):
        pipeline = ETLPipeline(extractor, transformer, loader)
        pipeline.run()

    with dag:
        PythonOperator(
            task_id='run_etl',
            python_callable=run_etl,
            provide_context=True,
        )

    return dag