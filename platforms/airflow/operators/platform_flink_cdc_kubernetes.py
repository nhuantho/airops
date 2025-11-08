import logging
from typing import Any

from airflow.utils.context import Context
from kubernetes.client import (
    ApiClient, V1ConfigMap, V1ConfigMapVolumeSource, V1Container, V1ObjectMeta, V1PodSpec, V1PodTemplate,
    V1PodTemplateSpec, V1Volume, V1VolumeMount,
)

from platforms.airflow.operators.platform_flink_kubernetes import PlatformFlinkKubernetesOperator
from platforms.kubernetes.models.flink_deployment import JobSpec, JobStateEnum, UpgradeModeEnum
from platforms.utils.consts import K8S_API_VERSION
from platforms.utils.secrets import get_connection

logger = logging.getLogger(__name__)


class PlatformFlinkCdcKubernetesOperator(PlatformFlinkKubernetesOperator):

    def __init__(
        self,
        source_db_conn_id: str,
        sink_db_conn_id: str,
        flink_cdc_config: dict[str, Any],
        source_config: dict[str, Any],
        sink_config: dict[str, Any],
        pipeline_config: dict[str, Any],
        flink_cdc_version: str,
        flink_cdc_main_application_file: str = None,
        **kwargs: Any
    ):
        super().__init__(**kwargs)
        self.source_db_conn_id = source_db_conn_id
        self.sink_db_conn_id = sink_db_conn_id
        self.flink_cdc_config = flink_cdc_config
        self.source_config = source_config
        self.sink_config = sink_config
        self.pipeline_config = pipeline_config
        self.flink_cdc_version = flink_cdc_version
        self.flink_cdc_main_application_file = flink_cdc_main_application_file

    def _source(self) -> dict[str, str]:
        self.log.info(f"Source conn: {self.sink_db_conn_id}")

        conn = get_connection(self.sink_db_conn_id)
        source_config = dict(
            type = conn.conn_type,
            hostname = conn.host,
            port = str(conn.port),
            username = conn.username,
            password = conn.password,
        )

        if self.source_config:
            source_config.update(self.source_config)

        self.log.info(f"Source config: {source_config}")

        return source_config

    def _sink(self) -> dict[str, str]:
        self.log.info(f"Sink conn: {self.sink_db_conn_id}")

        conn = get_connection(self.sink_db_conn_id)
        sink_config = conn.extra_dejson

        if self.sink_config:
            sink_config.update(self.source_config)

        self.log.info(f"Sink config: {sink_config}")

        return sink_config

    def _create_fink_cdc_config_map(self, context: Context):
        self.log.info(
            f"Config map for fink-cdc with Context: {self.cluster_context} and op_context: {context}"
        )

        config_map = ApiClient().sanitize_for_serialization(
            V1ConfigMap(
                api_version=K8S_API_VERSION,
                kind="ConfigMap",
                metadata=V1ObjectMeta(
                    name=f"configmap_{self.pod_name}",
                    namespace=self.namespace,
                ),
                data={
                    "flink-cdc.yaml": self.flink_cdc_config,
                    self.flink_cdc_main_application_file: {
                        "source": self.source(),
                        "sink": self.sink(),
                    },
                    "pipeline": self.pipeline_config,
                }
            )
        )

        self.hook.custom_object_client.list_cluster_custom_object(
            group="", version=K8S_API_VERSION, plural="configmaps",
        )

        self.log.info(f"body=config_map: {config_map}")
        response = self.hook.create_custom_object(
            group="",
            version=K8S_API_VERSION,
            plural="configmaps",
            body=config_map,
            namespace=self.namespace,
        )
        return response

    def _amount_flink_cdc_config_map(self):
        self.log.info("Mount configmap of fink-cdc to flink deployment")

        return V1PodTemplate(
            api_version=K8S_API_VERSION,
            kind="ConfigMap",
            template=V1PodTemplateSpec(
                spec=V1PodSpec(
                    containers=[V1Container(
                        name="flink-main-container", # don't modify this name,
                        volume_mounts=[V1VolumeMount(
                            name=f"config_{self.pod_name}",
                            mount_path=f"/opt/flink/flink-cdc-{self.flink_cdc_version}/conf",
                        )]
                    )],
                    volumes = [V1Volume(
                        name=f"configmap_{self.pod_name}",
                        config_map=V1ConfigMapVolumeSource(
                            name=f"config_{self.pod_name}",
                        )
                    )]
                )
            )
        )

    def _setup_job(self):
        if self.job is None:
            # default config job
            self.job = JobSpec(
                args=["--use-mini-cluster", f"/opt/flink/flink-cdc-{self.flink_cdc_version}/conf/{self.flink_cdc_main_application_file}"],
                entry_class="org.apache.flink.cdc.cli.CliFrontend",
                jar_uri=f"local:///opt/flink/lib/flink-cdc-dist-{self.flink_cdc_version}.jar",
                parallelism=1,
                state=JobStateEnum.RUNNING,
                upgrade_mode=UpgradeModeEnum.SAVEPOINT,
            )


    def execute(self, context: Context) -> Any:
        try:
            self.create_fink_cdc_config_map(context)
        except Exception as e:
            self.log.error(e)
            assert ValueError("Cannot create configmap")

        self.pod_template = self._amount_flink_cdc_config_map()
        self._setup_job()

        super().execute(context)
