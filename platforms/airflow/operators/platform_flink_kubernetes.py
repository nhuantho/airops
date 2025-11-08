import logging
from typing import Any

from airflow.providers.apache.flink.operators.flink_kubernetes import FlinkKubernetesOperator
from airflow.utils.context import Context
from kubernetes.client import (
    ApiClient, V1ObjectMeta, V1PodTemplate,
)

from platforms.kubernetes.models.flink_deployment import (
    FlinkDeployment, FlinkDeploymentSpec, FlinkVersion, IngressSpec, JobManagerSpec, JobSpec, KubernetesDeploymentMode,
    TaskManagerSpec,
)
from platforms.utils.utils import FlinkDeploymentMutatingHook

logger = logging.getLogger(__name__)


MAX_OBJECT_NAME_LEN = 253


class PlatformFlinkKubernetesOperator(FlinkKubernetesOperator):

    def __init__(
        self,
        mutating_hooks: list[FlinkDeploymentMutatingHook] = None,
        namespace: str = "flink",
        pod_name: str = None,
        job: JobSpec = None,
        restart_nonce: int = None,
        flink_configuration: dict[str, str] = None,
        image: str = None,
        image_pull_policy: str = None,
        service_account: str = None,
        flink_version: FlinkVersion = None,
        ingress: IngressSpec = None,
        pod_template: V1PodTemplate = None,
        job_manager: JobManagerSpec = None,
        task_manager: TaskManagerSpec = None,
        log_configuration: dict[str, str] = None,
        mode: KubernetesDeploymentMode = None,
        **kwargs: Any
    ):
        super().__init__(namespace=namespace, **kwargs)
        self.pod_name = pod_name
        self.mutating_hooks = mutating_hooks
        self.job = job
        self.restart_nonce = restart_nonce
        self.flink_configuration = flink_configuration
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.service_account = service_account
        self.flink_version = flink_version
        self.ingress = ingress
        self.pod_template = pod_template
        self.job_manager = job_manager
        self.task_manager = task_manager
        self.log_configuration = log_configuration
        self.mode = mode
        self.labels: dict[str, str] | None = None

    def _build_flink_deploment(self, context: Context) -> FlinkDeployment:
        mutating_hooks = []

        if self.mutating_hooks is not None:
            mutating_hooks.extend(self.mutating_hooks.copy())

        flink_deployment = FlinkDeployment(
            metadata=V1ObjectMeta(
                namespace=self.namespace,
                labels=self.labels,
                name=self.pod_name,
            ),
            spec=FlinkDeploymentSpec(
                job=self.job,
                restart_nonce=self.restart_nonce,
                flink_configuration=self.flink_configuration,
                image=self.image,
                image_pull_policy=self.image_pull_policy,
                service_account=self.service_account,
                flink_version=self.flink_version,
                ingress=self.ingress,
                pod_template=self.pod_template,
                job_manager=self.job_manager,
                task_manager=self.task_manager,
                log_configuration=self.log_configuration,
                mode=self.mode,
            ),
        )

        for hook in mutating_hooks:
            hook(flink_deployment, context)

        return flink_deployment


    def execute(self, context: Context) -> Any:
        self.application_file = ApiClient().sanitize_for_serialization(self._build_spark_application(context))

        super().execute(context)
