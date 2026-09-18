from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    """Inisialisasi Spark session dengan konfigurasi standar"""

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config('spark.driver.memory', '4g')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.sources.partitionOverwriteMode', 'dynamic')
        .getOrCreate()
    )

