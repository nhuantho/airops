from kubernetes.client.models import (
    V1HostPathVolumeSource, V1Volume, V1VolumeMount,
)

WAREHOUSE_REPO_MASTER_BRANCH_VOLUME = V1Volume(
    name="code",
    host_path=V1HostPathVolumeSource(
        path="/opt/warehouse",
        type="DirectoryOrCreate",
    ),
)

CODE_VOLUME_MOUNT = V1VolumeMount(
    name="code",
    mount_path="/app",
    read_only=True,
)
