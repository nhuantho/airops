from typing import Union

from airflow.utils.context import Context
from kubernetes.client import (
    V1EmptyDirVolumeSource, V1EnvVar, V1Volume, V1VolumeMount,
)

from platforms.kubernetes.models.spark_application import (
    DriverSpec, ExecutorSpec, SparkApplication,
)
from platforms.utils.consts import CONN_ID_STORAGE_CEPH_APPS_SPARK
from platforms.utils.secrets import get_iceberg_catalog_opts, get_s3_opts
from platforms.utils.utils import SparkApplicationMutatingHook
from platforms.utils.volumes import CODE_VOLUME_MOUNT, WAREHOUSE_REPO_MASTER_BRANCH_VOLUME

DEFAULT_ENV = [
    V1EnvVar(name="HADOOP_OPTIONAL_TOOLS", value="hadoop-aws"),
    V1EnvVar(name="PYTHONPATH", value="/app/warehouse/"),
]

EVENTLOG_BUCKET = "apps-spark"
EVENTLOG_CONN_ID = CONN_ID_STORAGE_CEPH_APPS_SPARK


def add_event_log_config() -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        prefix = f"fs.s3a.bucket.{EVENTLOG_BUCKET}"
        s3_opts = get_s3_opts(EVENTLOG_CONN_ID)

        if spark_application.spec.hadoop_conf is None:
            spark_application.spec.hadoop_conf = {}
        spark_application.spec.hadoop_conf.update({
            f"{prefix}.endpoint": s3_opts.endpoint,
            f"{prefix}.access.key": s3_opts.access_key,
            f"{prefix}.secret.key": s3_opts.secret_key,
            f"{prefix}.path.style.access": "true",
            f"{prefix}.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        })

        if spark_application.spec.spark_conf is None:
            spark_application.spec.spark_conf = {}
        spark_application.spec.spark_conf.update({
            "spark.eventLog.dir": f"s3a://{EVENTLOG_BUCKET}/spark-events/",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.compress": "true",
            # "spark.eventLog.rolling.enabled": "true",
            # "spark.eventLog.rolling.maxFileSize": "128M",
            # "spark.eventLog.logStageExecutorMetrics": "true",
        })

    return func


def add_prometheus_metrics_config() -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        if spark_application.spec.spark_conf is None:
            spark_application.spec.spark_conf = {}
        spark_application.spec.spark_conf.update({
            'spark.ui.prometheus.enabled': 'true',
            'spark.executor.processTreeMetrics.enabled': 'true',
        })

    return func

def add_airflow_metadata_info() -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        """ Provide airflow task_instance info to env so that spark app can use them to trace and analysis """

        task_instance = context['ti']
        for s in [spark_application.spec.driver, spark_application.spec.executor]:
            if s.env is None:
                s.env = []

            s.env.extend([
                V1EnvVar(name='AIRFLOW_DAG_ID', value=task_instance.dag_id),
                V1EnvVar(name='AIRFLOW_TASK_ID', value=task_instance.task_id),
                V1EnvVar(name='AIRFLOW_RUN_ID', value=task_instance.run_id),
            ])
    return func


def apply_default_volume_config() -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        if spark_application.spec.volumes is None:
            spark_application.spec.volumes = []
        if not any(volume.name == "code" for volume in spark_application.spec.volumes):
            spark_application.spec.volumes.append(WAREHOUSE_REPO_MASTER_BRANCH_VOLUME)

        for s in [spark_application.spec.driver, spark_application.spec.executor]:
            if s.volume_mounts is None:
                s.volume_mounts = []
            s.volume_mounts.append(CODE_VOLUME_MOUNT)

    return func


def apply_default_env_config() -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        for s in [spark_application.spec.driver, spark_application.spec.executor]:
            if s.env is None:
                s.env = []
            s.env.extend(DEFAULT_ENV)

    return func

def enable_apache_arrow() -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        if spark_application.spec.spark_conf is None:
            spark_application.spec.spark_conf = {}
        spark_application.spec.spark_conf.update({
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
            "spark.sql.execution.arrow.sparkr.enabled": "true",
            "spark.sql.execution.arrow.sparkr.fallback.enabled": "true",
        })

    return func


def enable_apache_hive(iceberg_catalog_conn_id: str) -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        catalog_opts = get_iceberg_catalog_opts(iceberg_catalog_conn_id)
        if spark_application.spec.spark_conf is None:
            spark_application.spec.spark_conf = {}
        spark_application.spec.spark_conf.update({
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": catalog_opts.warehouse,
            "spark.hadoop.hive.metastore.uris": catalog_opts.uri,
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        })

    return func


def enable_apache_iceberg(s3_conn_id: str, iceberg_catalog_conn_id: str, is_set_default_catalog: bool = False) -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        prefix = "spark.sql.catalog"
        s3_opts = get_s3_opts(s3_conn_id)
        catalog_opts = get_iceberg_catalog_opts(iceberg_catalog_conn_id)
        if spark_application.spec.spark_conf is None:
            spark_application.spec.spark_conf = {}
        spark_application.spec.spark_conf.update({
            f"{prefix}.{catalog_opts.catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"{prefix}.{catalog_opts.catalog_name}.type": catalog_opts.catalog_impl,
            f"{prefix}.{catalog_opts.catalog_name}.uri": catalog_opts.uri,
            f"{prefix}.{catalog_opts.catalog_name}.warehouse": catalog_opts.warehouse,
            f"{prefix}.{catalog_opts.catalog_name}.io-impl": catalog_opts.io_impl,
            f"{prefix}.{catalog_opts.catalog_name}.hadoop.fs.s3a.endpoint": s3_opts.endpoint, #https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration
            f"{prefix}.{catalog_opts.catalog_name}.hadoop.fs.s3a.access.key": s3_opts.access_key,
            f"{prefix}.{catalog_opts.catalog_name}.hadoop.fs.s3a.secret.key": s3_opts.secret_key,
            f"{prefix}.{catalog_opts.catalog_name}.hadoop.fs.s3a.path.style.access": "true",
            "spark.sql.storeAssignmentPolicy": "ANSI", #https://spark.apache.org/docs/3.5.2/sql-ref-ansi-compliance.html
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.iceberg.handle-timestamp-without-timezone": "true"
        })
        if is_set_default_catalog:
            spark_application.spec.spark_conf.update({
                "spark.sql.defaultCatalog": catalog_opts.catalog_name
            })

    return func


def config_app_logging() -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        specs: list[Union[DriverSpec, ExecutorSpec]] = [spark_application.spec.driver, spark_application.spec.executor]
        for s in specs:
            if s.env is None:
                s.env = []
            s.env.append(V1EnvVar(name="PYTHON_LOGGING_CONFIG", value="/app/warehouse/conf/log4p.yaml"))
            s.java_options = "-Dlog4j.configuration=file:///app/warehouse/conf/log4j.properties"

    return func


def optimize_io() -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        """
        ref:
        * https://spark.apache.org/docs/latest/sql-performance-tuning.html
        """
        if spark_application.spec.spark_conf is None:
            spark_application.spec.spark_conf = {}
        spark_application.spec.spark_conf.setdefault("spark.sql.files.maxRecordsPerFile", str(1_000_000))
        spark_application.spec.spark_conf.update({
            "spark.sql.sources.ignoreDataLocality": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        })

    return func


def limit_spill_data(size_limit: str = "32G") -> SparkApplicationMutatingHook:
    def func(spark_app: SparkApplication, context: Context) -> None:
        """
        ref: https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/user-guide.md#using-volume-for-scratch-space
        """
        if spark_app.spec.volumes is None:
            spark_app.spec.volumes = []
        spark_app.spec.volumes.append(V1Volume(
            name="spark-local-dir-1",
            empty_dir=V1EmptyDirVolumeSource(
                size_limit=size_limit
            )
        ))
        executor = spark_app.spec.executor
        if executor.volume_mounts is None:
            executor.volume_mounts = []
        executor.volume_mounts.append(V1VolumeMount(
            name="spark-local-dir-1",
            mount_path="/tmp/spark-local-dir"
        ))

    return func


def configure_default_datetime_timestamp_rebase() -> SparkApplicationMutatingHook:
    def func(spark_application: SparkApplication, context: Context) -> None:
        """Configure Spark to handle legacy datetime & timestamp using CORRECTED rebase mode, which is keep value as it is.
        Spark 3.1: https://spark.apache.org/docs/latest/sql-migration-guide.html
        """

        if spark_application.spec.spark_conf is None:
            spark_application.spec.spark_conf = {}

        # Spark v3.2.x +
        spark_application.spec.spark_conf.update({
            "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
            "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
            "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
            "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED"
        })

    return func

def add_s3_config(conn_id: str, bucket: str = None) -> SparkApplicationMutatingHook:
    def func(spark_app: SparkApplication, context: Context) -> None:
        if spark_app.spec.hadoop_conf is None:
            spark_app.spec.hadoop_conf = {}
        hadoop_conf = spark_app.spec.hadoop_conf
        prefix = f"fs.s3a.bucket.{bucket}" if bucket else "fs.s3a"
        s3_opts = get_s3_opts(conn_id)

        hadoop_conf.update({
            f"{prefix}.endpoint": s3_opts.endpoint,
            f"{prefix}.access.key": s3_opts.access_key,
            f"{prefix}.secret.key": s3_opts.secret_key,
            f"{prefix}.path.style.access": "true",
            f"{prefix}.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        })

    return func
