"""
SparkSession Factory Module

Provides factory functions for creating SparkSession instances.
"""
from pyspark.sql import SparkSession
from typing import Optional


def create_spark_session(
    app_name: str,
    master: str = "local[*]",
    config_overrides: Optional[dict] = None
) -> SparkSession:
    """
    Create and return a configured SparkSession.
    
    Args:
        app_name: Name for the Spark application
        master: Spark master URL ("local[*]" or "spark://spark-master:7077")
        config_overrides: Optional dict of Spark configurations
        
    Returns:
        Configured SparkSession instance
    """
    # DONE: Implement
    spark_session_builder = SparkSession.builder.appName(app_name).master(master)

    if config_overrides is not None:
        for key, value in config_overrides.items():
            spark_session_builder = spark_session_builder.config(key, value)

    return spark_session_builder.getOrCreate()
