import time
from pathlib import Path
import json

import grpc
import psycopg
import pytest
import requests
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf import message_factory
from psycopg.rows import dict_row
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage
from testcontainers.core.network import Network
from testcontainers.postgres import PostgresContainer

ROOT = Path(__file__).resolve().parents[1]


class ScipioCluster:
    def __init__(
        self,
        session,
        first_base_url,
        second_base_url,
        postgres_connection_string,
        first_grpc_target,
        second_grpc_target,
    ):
        self.session = session
        self.first_base_url = first_base_url
        self.second_base_url = second_base_url
        self.postgres_connection_string = postgres_connection_string
        self.first_grpc_target = first_grpc_target
        self.second_grpc_target = second_grpc_target

    def __iter__(self):
        yield self.session
        yield self.first_base_url
        yield self.second_base_url
        yield self.postgres_connection_string


@pytest.fixture(scope="session")
def container_base_url():
    def _container_base_url(container):
        host = container.get_container_host_ip()
        port = container.get_exposed_port(8080)
        return f"http://{host}:{port}"

    return _container_base_url


@pytest.fixture(scope="session")
def wait_for_health():
    def _wait_for_health(session, base_url):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                response = session.get(f"{base_url}/healthz", timeout=1)
                if response.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.2)

        pytest.fail("scipio did not become ready")

    return _wait_for_health


@pytest.fixture(scope="session")
def start_saga():
    def _start_saga(session, base_url, workflow, context):
        response = session.post(
            f"{base_url}/sagas",
            json={"workflow": workflow, "context": context},
            timeout=3,
        )
        assert response.status_code == 202
        return response.json()["id"]

    return _start_saga


@pytest.fixture(scope="session")
def wait_for_status():
    def _wait_for_status(session, base_url, saga_id, expected_status):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            response = session.get(f"{base_url}/sagas/{saga_id}", timeout=2)
            if response.status_code != 200:
                time.sleep(0.2)
                continue

            body = response.json()
            if body["status"] == expected_status:
                return body

            time.sleep(0.2)

        pytest.fail(f"saga {saga_id} did not reach status {expected_status}")

    return _wait_for_status


@pytest.fixture(scope="session")
def cancel_saga():
    def _cancel_saga(session, base_url, saga_id):
        response = session.post(f"{base_url}/sagas/{saga_id}/cancel", timeout=3)
        return response

    return _cancel_saga


@pytest.fixture(scope="session")
def scipio_cluster(container_base_url, wait_for_health):
    session = requests.Session()
    session.trust_env = False

    with Network() as network:
        with PostgresContainer(
            "postgres:16-alpine",
            username="scipio",
            password="scipio",
            dbname="scipio",
            network=network,
            network_aliases=["postgres"],
        ) as postgres:
            postgres_connection_string = (
                f"postgresql://scipio:scipio@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/scipio"
            )
            with DockerContainer("redis:8-alpine", network=network, network_aliases=["redis"]).with_command(
                ["redis-server", "--port", "6379"]
            ):
                with DockerImage(path=str(ROOT), tag="scipio:test", dockerfile_path="Dockerfile", clean_up=True) as image:
                    env = {
                        "PG_CONN": "postgresql://scipio:scipio@postgres:5432/scipio?sslmode=disable",
                        "REDIS_CONN": "redis://redis:6379/0",
                    }

                    with DockerContainer(str(image), network=network).with_exposed_ports(8080, 9090).with_envs(**env) as first:
                        with DockerContainer(str(image), network=network).with_exposed_ports(8080, 9090).with_envs(**env) as second:
                            first_base_url = container_base_url(first)
                            second_base_url = container_base_url(second)
                            first_grpc_target = f"{first.get_container_host_ip()}:{first.get_exposed_port(9090)}"
                            second_grpc_target = f"{second.get_container_host_ip()}:{second.get_exposed_port(9090)}"
                            wait_for_health(session, first_base_url)
                            wait_for_health(session, second_base_url)
                            yield ScipioCluster(
                                session=session,
                                first_base_url=first_base_url,
                                second_base_url=second_base_url,
                                postgres_connection_string=postgres_connection_string,
                                first_grpc_target=first_grpc_target,
                                second_grpc_target=second_grpc_target,
                            )

    session.close()


@pytest.fixture
def db_transaction(scipio_cluster):
    _, _, _, postgres_connection_string = scipio_cluster
    connection = psycopg.connect(postgres_connection_string, autocommit=False, row_factory=dict_row)
    cursor = connection.cursor()
    cursor.execute("BEGIN")
    try:
        yield cursor
    finally:
        connection.rollback()
        cursor.close()
        connection.close()


@pytest.fixture
def db_connection(scipio_cluster):
    _, _, _, postgres_connection_string = scipio_cluster
    connection = psycopg.connect(postgres_connection_string, autocommit=True, row_factory=dict_row)
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        connection.close()


@pytest.fixture(scope="session")
def grpc_schema():
    descriptor_data = (ROOT / "gen/proto/saga.pb").read_bytes()
    file_descriptor_set = descriptor_pb2.FileDescriptorSet()
    file_descriptor_set.ParseFromString(descriptor_data)

    pool = descriptor_pool.DescriptorPool()
    for file_descriptor in file_descriptor_set.file:
        pool.Add(file_descriptor)

    namespace = "scipio.saga.v1"
    message_names = (
        "StartSagaRequest",
        "StartSagaResponse",
        "GetSagaRequest",
        "GetSagaResponse",
        "CancelSagaRequest",
        "CancelSagaResponse",
    )
    messages = {}
    for message_name in message_names:
        descriptor = pool.FindMessageTypeByName(f"{namespace}.{message_name}")
        messages[message_name] = message_factory.GetMessageClass(descriptor)

    return messages


@pytest.fixture(scope="session")
def grpc_targets(scipio_cluster):
    return scipio_cluster.first_grpc_target, scipio_cluster.second_grpc_target


@pytest.fixture(scope="session")
def start_saga_grpc(grpc_schema):
    request_cls = grpc_schema["StartSagaRequest"]
    response_cls = grpc_schema["StartSagaResponse"]
    method_name = "/scipio.saga.v1.SagaService/StartSaga"

    def _start_saga_grpc(target, workflow, context):
        payload = json.dumps(context).encode("utf-8")
        request = request_cls(workflow=workflow, context=payload)
        with grpc.insecure_channel(target, options=(("grpc.enable_http_proxy", 0),)) as channel:
            call = channel.unary_unary(
                method_name,
                request_serializer=lambda msg: msg.SerializeToString(),
                response_deserializer=response_cls.FromString,
            )
            response = call(request, timeout=3)
        return response.id

    return _start_saga_grpc


@pytest.fixture(scope="session")
def get_saga_grpc(grpc_schema):
    request_cls = grpc_schema["GetSagaRequest"]
    response_cls = grpc_schema["GetSagaResponse"]
    method_name = "/scipio.saga.v1.SagaService/GetSaga"

    def _get_saga_grpc(target, saga_id):
        request = request_cls(id=saga_id)
        with grpc.insecure_channel(target, options=(("grpc.enable_http_proxy", 0),)) as channel:
            call = channel.unary_unary(
                method_name,
                request_serializer=lambda msg: msg.SerializeToString(),
                response_deserializer=response_cls.FromString,
            )
            response = call(request, timeout=3)
        return response.saga

    return _get_saga_grpc


@pytest.fixture(scope="session")
def cancel_saga_grpc(grpc_schema):
    request_cls = grpc_schema["CancelSagaRequest"]
    response_cls = grpc_schema["CancelSagaResponse"]
    method_name = "/scipio.saga.v1.SagaService/CancelSaga"

    def _cancel_saga_grpc(target, saga_id):
        request = request_cls(id=saga_id)
        with grpc.insecure_channel(target, options=(("grpc.enable_http_proxy", 0),)) as channel:
            call = channel.unary_unary(
                method_name,
                request_serializer=lambda msg: msg.SerializeToString(),
                response_deserializer=response_cls.FromString,
            )
            response = call(request, timeout=3)
        return response.saga

    return _cancel_saga_grpc


@pytest.fixture(scope="session")
def wait_for_status_grpc(get_saga_grpc):
    def _wait_for_status_grpc(target, saga_id, expected_status):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            saga = get_saga_grpc(target, saga_id)
            status_name = saga.DESCRIPTOR.fields_by_name["status"].enum_type.values_by_number[saga.status].name
            if status_name == expected_status:
                return saga
            time.sleep(0.2)

        pytest.fail(f"saga {saga_id} did not reach status {expected_status}")

    return _wait_for_status_grpc
