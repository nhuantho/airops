from dataclasses import dataclass
from enum import StrEnum

from kubernetes.client import (
    V1IngressTLS, V1ObjectMeta, V1PodTemplate,
)

from platforms.kubernetes.models.kubernetes_object import KubernetesObject


@dataclass
class FlinkDeploymentMeta:
    version = "v1beta1"
    kind = "FlinkDeployment"
    api_group = "flink.apache.org"
    api_version = f"{api_group}/{version}"


class Resource(KubernetesObject):
    openapi_types = {
        "cpu": "float",
        "memory": "str",
        "ephemeralStorage": "str",
    }

    attribute_map = {
        "cpu": "cpu",
        "memory": "memory",
        "ephemeral_storage": "ephemeralStorage",
    }

    def __init__(
        self,
        cpu: float = None,
        memory: str = None,
        ephemeral_storage: str = None
    ):
        self.cpu = cpu
        self.memory = memory
        self.ephemeral_storage = ephemeral_storage


class JobStateEnum(StrEnum):
    RUNNING = "running"
    SUSPENDED = "suspended"


JobState = str # alias


class UpgradeModeEnum(StrEnum):
    SAVEPOINT = "savepoint"
    LAST_STATE = "last_state"
    STATELESS = "stateless"


UpgradeMode = str # alias


class JobSpec(KubernetesObject):
    openapi_types = {
        "jarURI": "str",
        "parallelism": "int",
        "entryClass": "str",
        "args": "list[str]",
        "state": "JobState",
        "savepointTriggerNonce": "int",
        "initialSavepointPath": "str",
        "checkpointTriggerNonce": "int",
        "upgradeMode": "UpgradeMode",
        "allowNonRestoredState": "bool",
        "savepointRedeployNonce": "int",
        "autoscalerResetNonce": "int"
    }

    attribute_map = {
        "jar_uri": "jarURI",
        "parallelism": "parallelism",
        "entry_class": "entryClass",
        "args": "args",
        "state": "state",
        "savepoint_trigger_nonce": "savepointTriggerNonce",
        "initial_savepoint_path": "initialSavepointPath",
        "checkpoint_trigger_nonce": "checkpointTriggerNonce",
        "upgrade_mode": "upgradeMode",
        "allow_non_restored_state": "allowNonRestoredState",
        "savepoint_redeploy_nonce": "savepointRedeployNonce",
        "autoscaler_reset_nonce": "autoscalerResetNonce"
    }

    def __init__(
        self,
        jar_uri: str = None,
        parallelism: int = None,
        entry_class: str = None,
        args: list[str] = None,
        state: JobState = None,
        savepoint_trigger_nonce: int = None,
        initial_savepoint_path: str = None,
        checkpoint_trigger_nonce: int = None,
        upgrade_mode: UpgradeMode = None,
        allow_non_restored_state: bool = None,
        savepoint_redeploy_nonce: int = None,
        autoscaler_reset_nonce: int = None
    ):
        self.jar_uri = jar_uri
        self.parallelism = parallelism
        self.entry_class = entry_class
        self.args = args
        self.state = state
        self.savepoint_trigger_nonce = savepoint_trigger_nonce
        self.initial_savepoint_path = initial_savepoint_path
        self.checkpoint_trigger_nonce = checkpoint_trigger_nonce
        self.upgrade_mode = upgrade_mode
        self.allow_non_restored_state = allow_non_restored_state
        self.savepoint_redeploy_nonce = savepoint_redeploy_nonce
        self.autoscaler_reset_nonce = autoscaler_reset_nonce


ConfigObjectNode = dict[str, str]


class FlinkVersionNum(StrEnum):
    V1_13 = "v1_13"
    V1_14 = "v1_14"
    V1_15 = "v1_15"
    V1_16 = "v1_16"
    V1_17 = "v1_17"
    V1_18 = "v1_18"
    V1_19 = "v1_19"
    V1_20 = "v1_20"
    V2_0 = "v2_0"
    V2_1 = "v2_1"
    V2_2 = "v2_2"


FlinkVersion = str # alias


class IngressSpec(KubernetesObject):
    openapi_types = {
        "template": "str",
        "className": "str",
        "annotations": "dict[str, str]",
        "tls": "V1IngressTLS"
    }

    attribute_map = {
        "template": "template",
        "class_name": "className",
        "annotations": "annotations",
        "tls": "tls"
    }

    def __init__(
        self,
        template: str = None,
        class_name: str = None,
        annotations: dict[str, str] = None,
        tls: V1IngressTLS = None
    ):
        self.template = template
        self.class_name = class_name
        self.annotations = annotations
        self.tls = tls


class JobManagerSpec(KubernetesObject):
    openapi_types = {
        "resource": "Resource",
        "replicas": "int",
        "podTemplate": "V1PodTemplate"
    }

    attribute_map = {
        "resource": "resource",
        "replicas": "replicas",
        "pod_template": "podTemplate",
    }

    def __init__(
        self,
        resource: Resource = None,
        replicas: int = None,
        pod_template: V1PodTemplate = None,
    ):
        self.resource = resource
        self.replicas = replicas
        self.pod_template = pod_template


class TaskManagerSpec(KubernetesObject):
    openapi_types = {
        "resource": "Resource",
        "replicas": "int",
        "podTemplate": "V1PodTemplate"
    }

    attribute_map = {
        "resource": "resource",
        "replicas": "replicas",
        "pod_template": "podTemplate",
    }

    def __init__(
        self,
        resource: Resource = None,
        replicas: int = None,
        pod_template: V1PodTemplate = None,
    ):
        self.resource = resource
        self.replicas = replicas
        self.pod_template = pod_template


class KubernetesDeploymentModeEnum(StrEnum):
    NATIVE = "native"
    STANDALONE = "standalone"


KubernetesDeploymentMode = str # alias


class FlinkDeploymentSpec(KubernetesObject):
    openapi_types = {
        "job": "JobSpec",
        "restartNonce": "int",
        "flinkConfiguration": "ConfigObjectNode",
        "image": "str",
        "imagePullPolicy": "str",
        "serviceAccount": "str",
        "flinkVersion": "FlinkVersion",
        "ingress": "IngressSpec",
        "podTemplate": "V1PodTemplate",
        "jobManager": "JobManagerSpec",
        "taskManager": "TaskManagerSpec",
        "logConfiguration": "dict[str, str]",
        "mode": "KubernetesDeploymentMode"
    }

    attribute_map = {
        "job": "job",
        "restart_nonce": "restartNonce",
        "flink_configuration": "flinkConfiguration",
        "image": "image",
        "image_pull_policy": "imagePullPolicy",
        "service_account": "serviceAccount",
        "flink_version": "flinkVersion",
        "ingress": "ingress",
        "pod_template": "podTemplate",
        "job_manager": "jobManager",
        "task_manager": "taskManager",
        "log_configuration": "logConfiguration",
        "mode": "mode"
    }

    def __init__(
        self,
        job: JobSpec = None,
        restart_nonce: int = None,
        flink_configuration: ConfigObjectNode = None,
        image: str = None,
        image_pull_policy: str = None,
        service_account: str = None,
        flink_version: FlinkVersion = None,
        ingress: IngressSpec = None,
        pod_template: V1PodTemplate = None,
        job_manager: JobManagerSpec = None,
        task_manager: TaskManagerSpec = None,
        log_configuration: dict[str, str] = None,
        mode: KubernetesDeploymentMode = None
    ):
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


class FlinkDeploymentStatus(KubernetesObject):
    pass


class FlinkDeployment(KubernetesObject):
    openapi_types = {
        "api_version": "str",
        "version": "str",
        "kind": "str",
        "api_group": "str",
        "metadata": "V1ObjectMeta",
        "spec": "SparkApplicationSpec",
        "status": "SparkApplicationStatus"
    }

    attribute_map = {
        "api_version": "apiVersion",
        "version": "version",
        "kind": "kind",
        "api_group": "apiGroup",
        "metadata": "metadata",
        "spec": "spec",
        "status": "status"
    }

    def __init__(
        self,
        metadata: V1ObjectMeta,
        spec: FlinkDeploymentSpec = None,
        status: FlinkDeploymentStatus = None,
        api_version: str = FlinkDeploymentMeta.api_version,
        version: str = FlinkDeploymentMeta.version,
        kind: str = FlinkDeploymentMeta.kind,
        api_group: str = FlinkDeploymentMeta.api_group,
    ):
        self.metadata = metadata
        self.spec = spec
        self.status = status
        self.api_version = api_version
        self.version = version
        self.kind = kind
        self.api_group = api_group
