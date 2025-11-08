from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from kubernetes.client import (
    V1Affinity, V1Container, V1EnvFromSource, V1EnvVar, V1HostAlias, V1IngressTLS, V1Lifecycle, V1ObjectMeta,
    V1PodDNSConfig, V1PodSecurityContext, V1PodTemplate, V1ResourceRequirements, V1SecurityContext, V1Service,
    V1Toleration, V1Volume, V1VolumeMount,
)

from platforms.kubernetes.models.kubernetes_object import KubernetesObject


@dataclass
class SparkApplicationCRD:
    version = "v1beta2"
    kind = "SparkApplication"
    api_group = "sparkoperator.k8s.io"
    api_version = f"{api_group}/{version}"


class SparkApplicationType(StrEnum):
    JAVA = "Java"
    PYTHON = "Python"
    R = "R"
    SCALA = "Scala"


class DeployMode(StrEnum):
    CLIENT = "client"
    CLUSTER = "cluster"
    IN_CLUSTER_CLIENT = "in-cluster-client"


class GPUSpec(KubernetesObject):
    openapi_types = {
        "name": "str",
        "quantity": "int"
    }

    attribute_map = {
        "name": "name",
        "quantity": "quantity"
    }

    def __init__(self, name: str, quantity: int):
        self.name = name
        self.quantity = quantity


class NamePath(KubernetesObject):
    openapi_types = {
        "name": "str",
        "path": "str"
    }

    attribute_map = {
        "name": "name",
        "path": "path"
    }

    def __init__(self, name: str, path: str):
        self.name = name
        self.path = path


class SecretType(StrEnum):
    GCP_SERVICE_ACCOUNT = "GCPServiceAccount"
    GENERIC = "Generic"
    HADOOP_DELEGATION_TOKEN = "HadoopDelegationToken"


class SecretInfo(KubernetesObject):
    openapi_types = {
        "name": "str",
        "secret": "str",
        "secret_type": "str"
    }

    attribute_map = {
        "name": "name",
        "secret": "secret",
        "secret_type": "secretType"
    }

    def __init__(self, name: str, secret: str, secret_type: str | None = None):
        self.name = name
        self.secret = secret
        self.secret_type = secret_type # SecretType


class NameKey(KubernetesObject):
    openapi_types = {
        "name": "str",
        "key": "str"
    }

    attribute_map = {
        "name": "name",
        "key": "key"
    }

    def __init__(self, name: str, key: str):
        self.name = name
        self.key = key


class SparkPodSpec(KubernetesObject):
    openapi_types = {
        "template": "V1PodTemplate",
        "cores": "int",
        "core_limit": "str",
        "memory": "str",
        "memory_limit": "str",
        "memory_overhead": "str",
        "gpu": "GPUSpec",
        "image": "str",
        "config_maps": "list[NamePath]",
        "secrets": "list[SecretInfo]",
        "env": "list[V1EnvVar]",
        "env_vars": "dict[str, str]",
        "env_from": "list[V1EnvFromSource]",
        "env_secret_key_fefs": "dict[str, NameKey]",
        "labels": "dict[str, str]",
        "annotations": "dict[str, str]",
        "volume_mounts": "list[V1VolumeMount]",
        "affinity": "V1Affinity",
        "tolerations": "list[V1Toleration]",
        "pod_security_context": "V1PodSecurityContext",
        "security_context": "V1SecurityContext",
        "scheduler_name": "str",
        "sidecars": "list[V1Container]",
        "init_containers": "list[V1Container]",
        "host_network": "bool",
        "node_selector": "dict[str, str]",
        "dns_config": "V1PodDNSConfig",
        "termination_grace_period_seconds": "int",
        "service_account": "str",
        "host_aliases": "list[V1HostAlias]",
        "share_process_namespace": "bool"
    }

    attribute_map = {
        "template": "template",
        "cores": "cores",
        "core_limit": "coreLimit",
        "memory": "memory",
        "memory_limit": "memoryLimit",
        "memory_overhead": "memoryOverhead",
        "gpu": "gpu",
        "image": "image",
        "config_maps": "configMaps",
        "secrets": "secrets",
        "env": "env",
        "env_vars": "envVars",
        "env_from": "envFrom",
        "env_secret_key_fefs": "envSecretKeyRefs",
        "labels": "labels",
        "annotations": "annotations",
        "volume_mounts": "volumeMounts",
        "affinity": "affinity",
        "tolerations": "tolerations",
        "pod_security_context": "podSecurityContext",
        "security_context": "securityContext",
        "scheduler_name": "schedulerName",
        "sidecars": "sidecars",
        "init_containers": "initContainers",
        "host_network": "hostNetwork",
        "node_selector": "nodeSelector",
        "dns_config": "dnsConfig",
        "termination_grace_period_seconds": "terminationGracePeriodSeconds",
        "service_account": "serviceAccount",
        "host_aliases": "hostAliases",
        "share_process_namespace": "shareProcessNamespace"
    }

    def __init__(
        self,
        template: V1PodTemplate | None = None,
        cores: int | None = None,
        core_limit: str | None = None,
        memory: str | None = None,
        memory_limit: str | None = None,
        memory_overhead: str | None = None,
        gpu: GPUSpec | None = None,
        image: str | None = None,
        config_maps: list[NamePath] | None = None,
        secrets: list[SecretInfo] | None = None,
        env: list[V1EnvVar] | None = None,
        env_vars: dict[str, str] | None = None,
        env_from: list[V1EnvFromSource] | None = None,
        env_secret_key_fefs: dict[str, NameKey] | None = None,
        labels: dict[str, str] | None = None,
        annotations: dict[str, str] | None = None,
        volume_mounts: list[V1VolumeMount] | None = None,
        affinity: V1Affinity | None = None,
        tolerations: list[V1Toleration] | None = None,
        pod_security_context: V1PodSecurityContext | None = None,
        security_context: V1SecurityContext | None = None,
        scheduler_name: str | None = None,
        sidecars: list[V1Container] | None = None,
        init_containers: list[V1Container] | None = None,
        host_network: bool | None = None,
        node_selector: dict[str, str] | None = None,
        dns_config: V1PodDNSConfig | None = None,
        termination_grace_period_seconds: int | None = None,
        service_account: str | None = None,
        host_aliases: list[V1HostAlias] | None = None,
        share_process_namespace: bool | None = None,
    ):
        self.template = template
        self.cores = cores
        self.core_limit = core_limit
        self.memory = memory
        self.memory_limit = memory_limit
        self.memory_overhead = memory_overhead
        self.gpu = gpu
        self.image = image
        self.config_maps = config_maps
        self.secrets = secrets
        self.env = env
        self.env_vars = env_vars
        self.env_from = env_from
        self.env_secret_key_fefs = env_secret_key_fefs
        self.labels = labels
        self.annotations = annotations
        self.volume_mounts = volume_mounts
        self.affinity = affinity
        self.tolerations = tolerations
        self.pod_security_context = pod_security_context
        self.security_context = security_context
        self.scheduler_name = scheduler_name
        self.sidecars = sidecars
        self.init_containers = init_containers
        self.host_network = host_network
        self.node_selector = node_selector
        self.dns_config = dns_config
        self.termination_grace_period_seconds = termination_grace_period_seconds
        self.service_account = service_account
        self.host_aliases = host_aliases
        self.share_process_namespace = share_process_namespace


class Port(KubernetesObject):
    openapi_types = {
        "name": "str",
        "protocol": "str",
        "container_port": "str"
    }

    attribute_map = {
        "name": "name",
        "protocol": "protocol",
        "container_port": "containerPort"
    }

    def __init__(self, name: str, protocol: str, container_port: str):
        self.name = name
        self.protocol = protocol
        self.container_port = container_port


class DriverSpec(SparkPodSpec):
    openapi_types = {
        **SparkPodSpec.openapi_types,
        "pod_name": "str",
        "core_request": "str",
        "java_options": "str",
        "lifecycle": "V1Lifecycle",
        "kubernetes_master": "str",
        "service_annotations": "dict[str, str]",
        "service_labels": "dict[str, str]",
        "ports": "list[Port]",
        "priority_class_name": "str"
    }

    attribute_map = {
        **SparkPodSpec.attribute_map,
        "pod_name": "podName",
        "core_request": "coreRequest",
        "java_options": "javaOptions",
        "lifecycle": "lifecycle",
        "kubernetes_master": "kubernetesMaster",
        "service_annotations": "serviceAnnotations",
        "service_labels": "serviceLabels",
        "ports": "ports",
        "priority_class_name": "priorityClassName"
    }

    def __init__(
        self,
        spark_pod_spec: SparkPodSpec = SparkPodSpec(),
        pod_name: str | None = None,
        core_request: str | None = None,
        java_options: str | None = None,
        lifecycle: V1Lifecycle | None = None,
        kubernetes_master: str | None = None,
        service_annotations: dict[str, str] | None = None,
        service_labels: dict[str, str] | None = None,
        ports: list[Port] | None = None,
        priority_class_name: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.spark_pod_spec = spark_pod_spec
        self.pod_name = pod_name
        self.core_request = core_request
        self.java_options = java_options
        self.lifecycle = lifecycle
        self.kubernetes_master = kubernetes_master
        self.service_annotations = service_annotations
        self.service_labels = service_labels
        self.ports = ports
        self.priority_class_name = priority_class_name


class ExecutorSpec(SparkPodSpec):
    openapi_types = {
        **SparkPodSpec.openapi_types,
        "instances": "int",
        "core_request": "str",
        "java_options": "str",
        "lifecycle": "V1Lifecycle",
        "delete_on_termination": "bool",
        "ports": "list[Port]",
        "priority_class_name": "str"
    }

    attribute_map = {
        **SparkPodSpec.attribute_map,
        "instances": "instances",
        "core_request": "coreRequest",
        "java_options": "javaOptions",
        "lifecycle": "lifecycle",
        "delete_on_termination": "deleteOnTermination",
        "ports": "ports",
        "priority_class_name": "priorityClassName"
    }

    def __init__(
        self,
        spark_pod_spec: SparkPodSpec = SparkPodSpec(),
        instances: int | None = None,
        core_request: str | None = None,
        java_options: str | None = None,
        lifecycle: V1Lifecycle | None = None,
        delete_on_termination: bool | None = None,
        ports: list[Port] | None = None,
        priority_class_name: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.spark_pod_spec = spark_pod_spec
        self.instances = instances
        self.core_request = core_request
        self.java_options = java_options
        self.lifecycle = lifecycle
        self.delete_on_termination = delete_on_termination
        self.ports = ports
        self.priority_class_name = priority_class_name


class Dependencies(KubernetesObject):
    openapi_types = {
        "jars": "list[str]",
        "files": "list[str]" ,
        "py_files": "list[str]",
        "packages": "list[str]",
        "exclude_packages": "list[str]",
        "repositories": "list[str]",
        "archives": "list[str]"
    }

    attribute_map = {
        "jars": "jars",
        "files": "files",
        "py_files": "pyFiles",
        "packages": "packages",
        "exclude_packages": "excludePackages",
        "repositories": "repositories",
        "archives": "archives"
    }

    def __init__(
        self,
        jars: list[str] | None = None,
        files: list[str] | None = None,
        py_files: list[str] | None = None,
        packages: list[str] | None = None,
        exclude_packages: list[str] | None = None,
        repositories: list[str] | None = None,
        archives: list[str] | None = None,
    ):
        self.jars = jars
        self.files = files
        self.py_files = py_files
        self.packages = packages
        self.exclude_packages = exclude_packages
        self.repositories = repositories
        self.archives = archives


class RestartPolicyType(StrEnum):
    ALWAYS = "always"
    NEVER = "never"
    ON_FAILURE = "OnFailure"


class RestartPolicy(KubernetesObject):
    openapi_types = {
        "type": "str",
        "on_submission_failure_retries": "int",
        "on_failure_retries": "int",
        "on_submission_failure_retry_interval": "int",
        "on_failure_retry_interval": "int"
    }

    attribute_map = {
        "type": "type",
        "on_submission_failure_retries": "onSubmissionFailureRetries",
        "on_failure_retries": "onFailureRetries",
        "on_submission_failure_retry_interval": "onSubmissionFailureRetryInterval",
        "on_failure_retry_interval": "onFailureRetryInterval"
    }

    def __init__(
        self,
        type: str | None = None,
        on_submission_failure_retries: int | None = None,
        on_failure_retries: int | None = None,
        on_submission_failure_retry_interval: int | None = None,
        on_failure_retry_interval: int | None = None,
    ):
        self.type = type # RestartPolicyType
        self.on_submission_failure_retries = on_submission_failure_retries
        self.on_failure_retries = on_failure_retries
        self.on_submission_failure_retry_interval = on_submission_failure_retry_interval
        self.on_failure_retry_interval = on_failure_retry_interval


class PrometheusSpec(KubernetesObject):
    openapi_types = {
        "jmx_exporter_jar": "str",
        "port": "int",
        "port_name": "str",
        "config_file": "str",
        "configuration": "str"
    }

    attribute_map = {
        "jmx_exporter_jar": "jmxExporterJar",
        "port": "port",
        "port_name": "portName",
        "config_file": "configFile",
        "configuration": "configuration"
    }

    def __init__(
        self,
        jmx_exporter_jar: str | None = None,
        port: int | None = None,
        port_name: str | None = None,
        config_file: str | None = None,
        configuration: str | None = None,
    ):
        self.jmx_exporter_jar = jmx_exporter_jar
        self.port = port
        self.port_name = port_name
        self.config_file = config_file
        self.configuration = configuration


class MonitoringSpec(KubernetesObject):
    openapi_types = {
        "expose_driver_metrics": "bool",
        "expose_executor_metrics": "bool",
        "metrics_properties": "str",
        "metrics_properties_file": "str",
        "prometheus": "PrometheusSpec"
    }

    attribute_map = {
        "expose_driver_metrics": "exposeDriverMetrics",
        "expose_executor_metrics": "exposeExecutorMetrics",
        "metrics_properties": "metricsProperties",
        "metrics_properties_file": "metricsPropertiesFile",
        "prometheus": "prometheus"
    }

    def __init__(
        self,
        expose_driver_metrics: bool | None = None,
        expose_executor_metrics: bool | None = None,
        metrics_properties: str | None = None,
        metrics_properties_file: str | None = None,
        prometheus: PrometheusSpec | None = None,
    ):
        self.expose_driver_metrics = expose_driver_metrics
        self.expose_executor_metrics = expose_executor_metrics
        self.metrics_properties = metrics_properties
        self.metrics_properties_file = metrics_properties_file
        self.prometheus = prometheus


class BatchSchedulerConfiguration(KubernetesObject):
    openapi_types = {
        "queue": "str",
        "priority_class_name": "str",
        "resources": "V1ResourceRequirements"
    }

    attribute_map = {
        "queue": "queue",
        "priority_class_name": "priorityClassName",
        "resources": "resources"
    }

    def __init__(
        self,
        queue: str | None = None,
        priority_class_name: str | None = None,
        resources: V1ResourceRequirements | None = None, # need to re-check
    ):
        self.queue = queue
        self.priority_class_name = priority_class_name
        self.resources = resources


class SparkUIConfiguration(KubernetesObject):
    openapi_types = {
        "service_port": "int",
        "service_port_name": "str",
        "service_type": "V1Service",
        "service_annotations": "dict[str, str]",
        "service_labels": "dict[str, str]",
        "ingress_annotations": "dict[str, str]",
        "ingress_tls": "list[V1IngressTLS]"
    }

    attribute_map = {
        "service_port": "servicePort",
        "service_port_name": "servicePortName",
        "service_type": "serviceType",
        "service_annotations": "serviceAnnotations",
        "service_labels": "serviceLabels",
        "ingress_annotations": "ingressAnnotations",
        "ingress_tls": "ingressTLS"
    }

    def __init__(
        self,
        service_port: int | None = None,
        service_port_name: str | None = None,
        service_type: V1Service | None = None, # need to re-check
        service_annotations: dict[str, str] | None = None,
        service_labels: dict[str, str] | None = None,
        ingress_annotations: dict[str, str] | None = None,
        ingress_tls: list[V1IngressTLS] | None = None,
    ):
        self.service_port = service_port
        self.service_port_name = service_port_name
        self.service_type = service_type
        self.service_annotations = service_annotations
        self.service_labels = service_labels
        self.ingress_annotations = ingress_annotations
        self.ingress_tls = ingress_tls


class DriverIngressConfiguration(KubernetesObject):
    openapi_types = {
        "service_port": "int",
        "service_port_name": "str",
        "service_type": "V1Service",
        "service_annotations": "dict[str, str]",
        "service_labels": "dict[str, str]",
        "ingress_url_format": "str",
        "ingress_annotations": "dict[str, str]",
        "ingress_tls": "list[V1IngressTLS]"
    }

    attribute_map = {
        "service_port": "servicePort",
        "service_port_name": "servicePortName",
        "service_type": "serviceType",
        "service_annotations": "serviceAnnotations",
        "service_labels": "serviceLabels",
        "ingress_url_format": "ingressURLFormat",
        "ingress_annotations": "ingressAnnotations",
        "ingress_tls": "ingressTLS"
    }

    def __init__(
        self,
        service_port: int | None = None,
        service_port_name: str | None = None,
        service_type: V1Service | None = None, # need to re-check
        service_annotations: dict[str, str] | None = None,
        service_labels: dict[str, str] | None = None,
        ingress_url_format: str | None = None,
        ingress_annotations: dict[str, str] | None = None,
        ingress_tls: list[V1IngressTLS] | None = None,
    ):
        self.service_port = service_port
        self.service_port_name = service_port_name
        self.service_type = service_type
        self.service_annotations = service_annotations
        self.service_labels = service_labels
        self.ingress_url_format = ingress_url_format
        self.ingress_annotations = ingress_annotations
        self.ingress_tls = ingress_tls


class DynamicAllocation(KubernetesObject):
    openapi_types = {
        "enabled": "bool",
        "initial_executors": "int",
        "min_executors": "int",
        "max_executors": "int",
        "shuffle_tracking_enabled": "bool",
        "shuffle_tracking_timeout": "int"
    }

    attribute_map = {
        "enabled": "enabled",
        "initial_executors": "initialExecutors",
        "min_executors": "minExecutors",
        "max_executors": "maxExecutors",
        "shuffle_tracking_enabled": "shuffleTrackingEnabled",
        "shuffle_tracking_timeout": "shuffleTrackingTimeout"
    }

    def __init__(
        self,
        enabled: bool | None = None,
        initial_executors: int | None = None,
        min_executors: int | None = None,
        max_executors: int | None = None,
        shuffle_tracking_enabled: bool | None = None,
        shuffle_tracking_timeout: int | None = None,
    ):
        self.enabled = enabled
        self.initial_executors = initial_executors
        self.min_executors = min_executors
        self.max_executors = max_executors
        self.shuffle_tracking_enabled = shuffle_tracking_enabled
        self.shuffle_tracking_timeout = shuffle_tracking_timeout


class SparkApplicationSpec(KubernetesObject):
    openapi_types = {
        "type": "str",
        "spark_version": "str",
        "mode": "str",
        "proxy_user": "str",
        "image": "str",
        "image_pull_policy": "str",
        "image_pull_secrets": "list[str]",
        "main_class": "str",
        "main_application_file": "str",
        "arguments": "list[str]",
        "spark_conf": "dict[str, str]",
        "hadoop_conf": "dict[str, str]",
        "spark_config_map": "str",
        "hadoop_config_map": "str",
        "volumes": "list[V1Volume]",
        "driver": "DriverSpec",
        "executor": "ExecutorSpec",
        "deps": "Dependencies",
        "restart_policy": "RestartPolicy",
        "node_selector": "dict[str, str]",
        "failure_retries": "int",
        "retry_interval": "int",
        "python_version": "str",
        "memory_overhead_factor": "str",
        "monitoring": "MonitoringSpec",
        "batch_scheduler": "str",
        "time_to_live_seconds": "int",
        "batch_scheduler_options": "BatchSchedulerConfiguration",
        "spark_ui_options": "SparkUIConfiguration",
        "driver_ingress_options": "list[DriverIngressConfiguration]",
        "dynamic_allocation": "DynamicAllocation"
    }

    attribute_map = {
        "type": "type",
        "spark_version": "sparkVersion",
        "mode": "mode",
        "proxy_user": "proxyUser",
        "image": "image",
        "image_pull_policy": "imagePullPolicy",
        "image_pull_secrets": "imagePullSecrets",
        "main_class": "mainClass",
        "main_application_file": "mainApplicationFile",
        "arguments": "arguments",
        "spark_conf": "sparkConf",
        "hadoop_conf": "hadoopConf",
        "spark_config_map": "sparkConfigMap",
        "hadoop_config_map": "hadoopConfigMap",
        "volumes": "volumes",
        "driver": "driver",
        "executor": "executor",
        "deps": "deps",
        "restart_policy": "restartPolicy",
        "node_selector": "nodeSelector",
        "failure_retries": "failureRetries",
        "retry_interval": "retryInterval",
        "python_version": "pythonVersion",
        "memory_overhead_factor": "memoryOverheadFactor",
        "monitoring": "monitoring",
        "batch_scheduler": "batchScheduler",
        "time_to_live_seconds": "timeToLiveSeconds",
        "batch_scheduler_options": "batchSchedulerOptions",
        "spark_ui_options": "sparkUIOptions",
        "driver_ingress_options": "driverIngressOptions",
        "dynamic_allocation": "dynamicAllocation"
    }

    def __init__(
        self,
        type: str = SparkApplicationType.PYTHON,
        spark_version: str | None = None,
        mode: str = DeployMode.CLUSTER,
        proxy_user: str | None = None,
        image: str | None = None,
        image_pull_policy: str | None = None,
        image_pull_secrets: list[str] | None = None,
        main_class: str | None = None,
        main_application_file: str | None = None,
        arguments: list[str] | None = None,
        spark_conf: dict[str, str] | None = None,
        hadoop_conf: dict[str, str] | None = None,
        spark_config_map: str | None = None,
        hadoop_config_map: str | None = None,
        volumes: list[V1Volume] | None = None,
        driver: DriverSpec = DriverSpec(),
        executor: ExecutorSpec = ExecutorSpec(),
        deps: Dependencies | None = None,
        restart_policy: RestartPolicy | None = None,
        node_selector: dict[str, str] | None = None,
        failure_retries: int | None = None,
        retry_interval: int | None = None,
        python_version: str | None = None,
        memory_overhead_factor: str | None = None,
        monitoring: MonitoringSpec | None = None,
        batch_scheduler: str | None = None,
        time_to_live_seconds: int | None = None,
        batch_scheduler_options: BatchSchedulerConfiguration | None = None,
        spark_ui_options: SparkUIConfiguration | None = None,
        driver_ingress_options: list[DriverIngressConfiguration] | None = None,
        dynamic_allocation: DynamicAllocation | None = None,
    ):
        self.type = type
        self.spark_version = spark_version
        self.mode = mode
        self.proxy_user = proxy_user
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.image_pull_secrets = image_pull_secrets
        self.main_class = main_class
        self.main_application_file = main_application_file
        self.arguments = arguments
        self.spark_conf = spark_conf
        self.hadoop_conf = hadoop_conf
        self.spark_config_map = spark_config_map
        self.hadoop_config_map = hadoop_config_map
        self.volumes = volumes
        self.driver = driver
        self.executor = executor
        self.deps = deps
        self.restart_policy = restart_policy
        self.node_selector = node_selector
        self.failure_retries = failure_retries
        self.retry_interval = retry_interval
        self.python_version = python_version
        self.memory_overhead_factor = memory_overhead_factor
        self.monitoring = monitoring
        self.batch_scheduler = batch_scheduler
        self.time_to_live_seconds = time_to_live_seconds
        self.batch_scheduler_options = batch_scheduler_options
        self.spark_ui_options = spark_ui_options
        self.driver_ingress_options = driver_ingress_options
        self.dynamic_allocation = dynamic_allocation


class DriverInfo(KubernetesObject):
    openapi_types = {
        "web_ui_service_name": "str",
        "web_ui_address": "str",
        "web_ui_port": "str",
        "web_ui_ingress_name": "str",
        "web_ui_ingress_address": "str",
        "pod_name": "str"
    }

    attribute_map = {
        "web_ui_service_name": "webUIServiceName",
        "web_ui_address": "webUIAddress",
        "web_ui_port": "webUIPort",
        "web_ui_ingress_name": "webUIIngressName",
        "web_ui_ingress_address": "webUIIngressAddress",
        "pod_name": "podName"
    }

    def __init__(
        self,
        web_ui_service_name: str | None = None,
        web_ui_address: str | None = None,
        web_ui_port: str | None = None,
        web_ui_ingress_name: str | None = None,
        web_ui_ingress_address: str | None = None,
        pod_name: str | None = None,
    ):
        self.web_ui_service_name = web_ui_service_name
        self.web_ui_address = web_ui_address
        self.web_ui_port = web_ui_port
        self.web_ui_ingress_name = web_ui_ingress_name
        self.web_ui_ingress_address = web_ui_ingress_address
        self.pod_name = pod_name


class ApplicationStateType(StrEnum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SUBMISSION_FAILED = "SUBMISSION_FAILED"
    FAILING = "FAILING"
    INVALIDATING = "INVALIDATING"
    PENDING_RERUN = "PENDING_RERUN"
    RUNNING = "RUNNING"
    SUBMITTED = "SUBMITTED"
    SUCCEEDING = "SUCCEEDED"
    UNKNOWN = "UNKNOWN"


class ApplicationState(KubernetesObject):
    openapi_types = {
        "state": "str",
        "error_message": "str"
    }

    attribute_map = {
        "state": "state",
        "error_message": "errorMessage"
    }

    def __init__(self, state: str | None = None, error_message: str | None = None):
        self.state = state # ApplicationStateType
        self.error_message = error_message


class ExecutorState(StrEnum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    UNKNOWN = "UNKNOWN"


class SparkApplicationStatus(KubernetesObject):
    openapi_types = {
        "spark_application_id": "str",
        "submission_id": "str",
        "last_submission_attempt_time": "datetime",
        "termination_time": "datetime",
        "driver_info": "DriverInfo",
        "application_state": "ApplicationState",
        "executor_state": "dict[str, str]",
        "execution_attempts": "int",
        "submission_attempts": "int"
    }

    attribute_map = {
        "spark_application_id": "sparkApplicationId",
        "submission_id": "submissionID",
        "last_submission_attempt_time": "lastSubmissionAttemptTime",
        "termination_time": "terminationTime",
        "driver_info": "driverInfo",
        "application_state": "applicationState",
        "executor_state": "executorState",
        "execution_attempts": "executionAttempts",
        "submission_attempts": "submissionAttempts"
    }

    def __init__(
        self,
        spark_application_id: str | None = None,
        submission_id: str | None = None,
        last_submission_attempt_time: datetime | None = None,
        termination_time: datetime | None = None,
        driver_info: DriverInfo | None = None,
        application_state: ApplicationState | None = None,
        executor_state: dict[str, str] | None = None,
        execution_attempts: int | None = None,
        submission_attempts: int | None = None,
    ):
        self.spark_application_id = spark_application_id
        self.submission_id = submission_id
        self.last_submission_attempt_time = last_submission_attempt_time
        self.termination_time = termination_time
        self.driver_info = driver_info
        self.application_state = application_state
        self.executor_state = executor_state # ExecutorState
        self.execution_attempts = execution_attempts
        self.submission_attempts = submission_attempts


class SparkApplication:
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
        spec: SparkApplicationSpec,
        status: SparkApplicationStatus | None = None,
        api_version: str = SparkApplicationCRD.api_version,
        version: str = SparkApplicationCRD.version,
        kind: str = SparkApplicationCRD.kind,
        api_group: str = SparkApplicationCRD.api_group,
    ):
        self.metadata = metadata
        self.spec = spec
        self.status = status
        self.api_version = api_version
        self.version = version
        self.kind = kind
        self.api_group = api_group


class SparkApplicationTemplateSpec(KubernetesObject, SparkApplication):
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
        api_version: str = SparkApplicationCRD.api_version,
        version: str = SparkApplicationCRD.version,
        kind: str = SparkApplicationCRD.kind,
        api_group: str = SparkApplicationCRD.api_group,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.api_version = api_version
        self.version = version
        self.kind = kind
        self.api_group = api_group
