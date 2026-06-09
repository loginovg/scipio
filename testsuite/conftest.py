import time
from pathlib import Path
import json
from contextlib import ExitStack

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
        step_grpc_target,
        step_http_base_url,
    ):
        self.session = session
        self.first_base_url = first_base_url
        self.second_base_url = second_base_url
        self.postgres_connection_string = postgres_connection_string
        self.first_grpc_target = first_grpc_target
        self.second_grpc_target = second_grpc_target
        self.step_grpc_target = step_grpc_target
        self.step_http_base_url = step_http_base_url

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
def start_saga(scipio_cluster):
    def _start_saga(session, base_url, workflow, context, steps=None, idempotency_key=None):
        if steps is None:
            steps = [{"name": workflow, "grpc_target": scipio_cluster.step_grpc_target}]

        payload = {"workflow": workflow, "context": context, "steps": steps}
        if idempotency_key is not None:
            payload["idempotency_key"] = idempotency_key

        response = session.post(
            f"{base_url}/sagas",
            json=payload,
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
            if body["status"] == "FAILED" and expected_status != "FAILED":
                pytest.fail(f"saga {saga_id} failed while waiting for {expected_status}: {body}")

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

    with ExitStack() as stack:
        network = stack.enter_context(Network())

        postgres = stack.enter_context(
            PostgresContainer(
                "postgres:16-alpine",
                username="scipio",
                password="scipio",
                dbname="scipio",
                network=network,
                network_aliases=["postgres"],
            )
        )

        postgres_connection_string = (
            f"postgresql://scipio:scipio@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/scipio"
        )

        stack.enter_context(
            DockerContainer("redis:8-alpine", network=network, network_aliases=["redis"]).with_command(
                ["redis-server", "--port", "6379"]
            )
        )

        image = stack.enter_context(DockerImage(path=str(ROOT), tag="scipio:test", dockerfile_path="Dockerfile", clean_up=True))
        step_executor_image = stack.enter_context(
            DockerImage(
                path=str(ROOT),
                tag="scipio-step-executor:test",
                dockerfile_path="testsuite/step_executor/Dockerfile",
                clean_up=True,
            )
        )

        step_executor = stack.enter_context(
            DockerContainer(str(step_executor_image), network=network)
            .with_exposed_ports(50051, 18080)
            .with_network_aliases("step-executor")
            .with_env("STEP_EXECUTOR_HTTP_PORT", "18080")
        )

        step_http_base_url = (
            f"http://{step_executor.get_container_host_ip()}:{step_executor.get_exposed_port(18080)}"
        )
        step_executor_ready = False
        step_executor_deadline = time.monotonic() + 30
        while time.monotonic() < step_executor_deadline:
            try:
                response = session.get(f"{step_http_base_url}/healthz", timeout=1)
                if response.status_code == 200:
                    step_executor_ready = True
                    break
            except requests.RequestException:
                pass
            time.sleep(0.2)

        if not step_executor_ready:
            stdout, stderr = step_executor.get_logs()
            pytest.fail(
                "step executor did not become ready\n"
                f"stdout: {stdout.decode('utf-8', errors='replace')}\n"
                f"stderr: {stderr.decode('utf-8', errors='replace')}"
            )

        env = {
            "SCIPIO_GRPC_PORT": "9090",
            "SCIPIO_HTTP_PORT": "8080",
            "SCIPIO_STEP_WORKERS": "8",
            "SCIPIO_STEP_POLL_INTERVAL": "25ms",
            "SCIPIO_STEP_STALE_TIMEOUT": "5s",
            "SCIPIO_LOCK_TTL": "5s",
            "SCIPIO_LOCK_RETRY_INTERVAL": "25ms",
            "PG_CONN": "postgresql://scipio:scipio@postgres:5432/scipio?sslmode=disable",
            "REDIS_CONN": "redis://redis:6379/0",
        }

        first = stack.enter_context(DockerContainer(str(image), network=network).with_exposed_ports(8080, 9090).with_envs(**env))
        second = stack.enter_context(DockerContainer(str(image), network=network).with_exposed_ports(8080, 9090).with_envs(**env))

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
            step_grpc_target="step-executor:50051",
            step_http_base_url=step_http_base_url,
        )

    session.close()


@pytest.fixture
def wait_for_step_dispatch(scipio_cluster):
    session = requests.Session()
    session.trust_env = False
    reset_response = session.delete(f"{scipio_cluster.step_http_base_url}/calls", timeout=3)
    assert reset_response.status_code == 204

    def _wait_for_step_dispatch(saga_id, expected_calls=1, operation=None):
        deadline = time.monotonic() + 20

        while time.monotonic() < deadline:
            response = session.get(f"{scipio_cluster.step_http_base_url}/calls", timeout=2)
            if response.status_code != 200:
                time.sleep(0.2)
                continue

            calls = response.json()
            saga_calls = [call for call in calls if call["saga_id"] == saga_id]
            if operation is not None:
                saga_calls = [call for call in saga_calls if call.get("operation") == operation]

            if len(saga_calls) >= expected_calls:
                return saga_calls

            time.sleep(0.2)

        pytest.fail(f"step executor did not receive expected calls for saga {saga_id}")

    try:
        yield _wait_for_step_dispatch
    finally:
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
        "StartSagaStep",
        "GetSagaRequest",
        "GetSagaResponse",
        "CancelSagaRequest",
        "CancelSagaResponse",
        "ExecuteStepRequest",
        "ExecuteStepResponse",
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
def start_saga_grpc(grpc_schema, scipio_cluster):
    request_cls = grpc_schema["StartSagaRequest"]
    step_cls = grpc_schema["StartSagaStep"]
    response_cls = grpc_schema["StartSagaResponse"]
    method_name = "/scipio.saga.v1.SagaService/StartSaga"

    def _start_saga_grpc(target, workflow, context, steps=None):
        if steps is None:
            steps = [{"name": workflow, "grpc_target": scipio_cluster.step_grpc_target}]

        payload = json.dumps(context).encode("utf-8")
        mapped_steps = [step_cls(name=step["name"], grpc_target=step["grpc_target"]) for step in steps]
        request = request_cls(workflow=workflow, context=payload, steps=mapped_steps)

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
            if status_name == "SAGA_STATUS_FAILED" and expected_status != "SAGA_STATUS_FAILED":
                raise AssertionError(f"saga {saga_id} failed while waiting for {expected_status}: {saga}")
            time.sleep(0.2)

        pytest.fail(f"saga {saga_id} did not reach status {expected_status}")

    return _wait_for_status_grpc
