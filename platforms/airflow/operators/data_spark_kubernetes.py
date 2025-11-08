import logging
from typing import Any

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.context import Context
from kubernetes.client import (
    ApiClient, V1EnvVar, V1ObjectMeta, V1Volume, V1VolumeMount,
)

from platforms.kubernetes.models.spark_application import (
    DeployMode, DriverSpec, ExecutorSpec, SparkApplication, SparkApplicationSpec, SparkApplicationType,
)
from platforms.kubernetes.models.spark_resource import (
    MICRO, SMALL, Resource,
)
from platforms.kubernetes.mutating_hooks.spark_hooks import (
    add_airflow_metadata_info, add_event_log_config, add_prometheus_metrics_config, add_s3_config,
    apply_default_env_config, apply_default_volume_config, config_app_logging,
    configure_default_datetime_timestamp_rebase, enable_apache_arrow, optimize_io,
)
from platforms.utils.consts import (
    CONN_ID_STORAGE_CEPH_APPS_SPARK, IMAGE_SPARK_3_5_ICEBERG_1_8, PYTHON_3, SPARK_VERSION_3_5,
)
from platforms.utils.utils import SparkApplicationMutatingHook

logger = logging.getLogger(__name__)


class DataSparkKubernetesOperator(SparkKubernetesOperator):

    def __init__(
        self,
        mutating_hooks: list[SparkApplicationMutatingHook] = None,
        namespace: str = "spark",
        spark_application_type: str = SparkApplicationType.PYTHON,
        spark_version: str = SPARK_VERSION_3_5,
        mode: str = DeployMode.CLUSTER,
        image: str = IMAGE_SPARK_3_5_ICEBERG_1_8,
        main_class: str | None = None,
        main_application_file: str = "",
        arguments: list[str] | None = None,
        spark_conf: dict[str, str] | None = None,
        hadoop_conf: dict[str, str] | None = None,
        volumes: list[V1Volume] | None = None,
        driver_resources: Resource = MICRO,
        executor_resources: Resource = SMALL,
        executor_instances: int = 1,
        env: list[V1EnvVar] | None = None,
        volume_mounts: list[V1VolumeMount] | None = None,
        python_version: str = PYTHON_3,
        time_to_live_seconds: int = 86400,
        **kwargs: Any
    ):
        super().__init__(namespace=namespace, **kwargs)
        self.mutating_hooks = mutating_hooks
        self.spark_application_type = spark_application_type
        self.spark_version = spark_version
        self.mode = mode
        self.image = image
        self.main_class = main_class
        self.main_application_file = main_application_file
        self.arguments = arguments or []
        self.spark_conf = spark_conf
        self.hadoop_conf = hadoop_conf
        self.volumes = volumes or []
        self.driver_resources = driver_resources
        self.executor_resources = executor_resources
        self.executor_instances = executor_instances
        self.env = env
        self.volume_mounts = volume_mounts or []
        self.python_version = python_version
        self.time_to_live_seconds = time_to_live_seconds
        self.labels: dict[str, str] | None = None


    def _build_spark_application(self, context: Context) -> SparkApplication:
        mutating_hooks = [
            add_event_log_config(),
            add_prometheus_metrics_config(),
            add_airflow_metadata_info(),
            add_s3_config(conn_id=CONN_ID_STORAGE_CEPH_APPS_SPARK),
            configure_default_datetime_timestamp_rebase(),
            optimize_io(),
            enable_apache_arrow()
        ]

        if self.spark_application_type == SparkApplicationType.PYTHON:
            self.main_application_file = f"local:///app/warehouse/{self.main_application_file}"
            mutating_hooks.extend([
                apply_default_volume_config(),
                apply_default_env_config(),
                config_app_logging(),
            ])

        if self.mutating_hooks is not None:
            mutating_hooks.extend(self.mutating_hooks.copy())

        spark_application = SparkApplication(
            metadata=V1ObjectMeta(
                namespace=self.namespace,
                labels=self.labels,
            ),
            spec=SparkApplicationSpec(
                type=self.spark_application_type,
                spark_version=self.spark_version,
                mode=self.mode,
                image=self.image,
                image_pull_policy="IfNotPresent",
                main_class=self.main_class,
                main_application_file=self.main_application_file,
                arguments=self.arguments,
                spark_conf=self.spark_conf,
                hadoop_conf=self.hadoop_conf,
                volumes = self.volumes,
                driver=DriverSpec(
                    cores=self.driver_resources.cores,
                    core_limit=self.driver_resources.core_limit,
                    memory=self.driver_resources.memory,
                    memory_limit=self.driver_resources.memory_limit,
                    memory_overhead=self.driver_resources.memory_overhead,
                    env=self.env.copy() if self.env else None,
                    volume_mounts=self.volume_mounts.copy() if self.volume_mounts else None,
                    service_account="spark",
                    core_request=self.driver_resources.core_request,
                ),
                executor = ExecutorSpec(
                    cores=self.executor_resources.cores,
                    core_limit=self.executor_resources.core_limit,
                    memory=self.executor_resources.memory,
                    memory_limit=self.executor_resources.memory_limit,
                    memory_overhead=self.executor_resources.memory_overhead,
                    env=self.env.copy() if self.env else None,
                    volume_mounts=self.volume_mounts.copy() if self.volume_mounts else None,
                    service_account="spark",
                    instances=self.executor_instances,
                    core_request=self.executor_resources.core_request,
                ),
                python_version=self.python_version,
                time_to_live_seconds=self.time_to_live_seconds,
            )
        )

        for hook in mutating_hooks:
            hook(spark_application, context)

        return spark_application


    def execute(self, context: Context) -> Any:
        self.labels = self._get_ti_pod_labels(context)

        self.template_spec = ApiClient().sanitize_for_serialization(self._build_spark_application(context))

        logger.info(f"Spark application template spec: {self.template_spec}")

        super().execute(context)
