# handles customizations for spark application
from collections.abc import Callable

from airflow.utils.context import Context

from platforms.kubernetes.models.flink_deployment import FlinkDeployment
from platforms.kubernetes.models.spark_application import SparkApplication

SparkApplicationMutatingHook = Callable[[SparkApplication, Context], None]
FlinkDeploymentMutatingHook = Callable[[FlinkDeployment, Context], None]
