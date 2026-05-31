import json

import grpc
import pytest


def test_should_create_and_complete_saga_when_grpc_start_request_is_valid(
    grpc_targets,
    start_saga_grpc,
    wait_for_status_grpc,
):
    # given
    first_target, _ = grpc_targets

    # when
    saga_id = start_saga_grpc(first_target, "grpc_order_flow", {"user_id": 10})
    saga = wait_for_status_grpc(first_target, saga_id, "SAGA_STATUS_COMPLETED")
    payload = json.loads(saga.context.decode("utf-8"))

    # then
    assert saga.id == saga_id
    assert saga.workflow == "grpc_order_flow"
    assert payload["user_id"] == 10


def test_should_share_saga_state_between_instances_when_grpc_get_is_called_on_another_instance(
    grpc_targets,
    start_saga_grpc,
    wait_for_status_grpc,
):
    # given
    first_target, second_target = grpc_targets

    # when
    saga_id = start_saga_grpc(first_target, "grpc_shared_flow", {"ref": "state"})
    saga = wait_for_status_grpc(second_target, saga_id, "SAGA_STATUS_COMPLETED")

    # then
    assert saga.id == saga_id
    assert saga.workflow == "grpc_shared_flow"


def test_should_dispatch_context_to_step_executor_when_grpc_saga_is_started(
    grpc_targets,
    start_saga_grpc,
    wait_for_status_grpc,
    wait_for_step_dispatch,
):
    # given
    first_target, second_target = grpc_targets
    saga_context = {"tenant": "acme", "amount": 99, "flags": ["new"]}

    # when
    saga_id = start_saga_grpc(first_target, "grpc_dispatch_ctx_flow", saga_context)
    saga = wait_for_status_grpc(second_target, saga_id, "SAGA_STATUS_COMPLETED")
    calls = wait_for_step_dispatch(saga_id, expected_calls=1)

    # then
    assert saga.id == saga_id
    assert len(calls) == 1
    assert calls[0]["saga_id"] == saga_id
    assert calls[0]["workflow"] == "grpc_dispatch_ctx_flow"
    assert calls[0]["step_name"] == "grpc_dispatch_ctx_flow"
    assert calls[0]["context"] == saga_context


def test_should_return_internal_error_when_grpc_cancel_is_requested_and_compensation_is_not_implemented(
    grpc_targets,
    start_saga_grpc,
    cancel_saga_grpc,
    wait_for_status_grpc,
):
    # given
    first_target, _ = grpc_targets
    saga_id = start_saga_grpc(first_target, "grpc_cancel_flow", {"ref": "cancel"})

    # when
    with pytest.raises(grpc.RpcError) as err:
        cancel_saga_grpc(first_target, saga_id)

    saga = wait_for_status_grpc(first_target, saga_id, "SAGA_STATUS_CANCELING")

    # then
    assert err.value.code() == grpc.StatusCode.INTERNAL
    assert err.value.details() == "saga compensation is not implemented"
    assert saga.id == saga_id
