import concurrent.futures
import uuid


def test_should_return_not_found_when_http_get_requested_for_missing_saga(scipio_cluster):
    session, first_base_url, _, _ = scipio_cluster

    response = session.get(f"{first_base_url}/sagas/{uuid.uuid4().hex}", timeout=3)

    assert response.status_code == 404
    assert response.json()["error"] == "saga not found"


def test_should_return_not_found_when_http_cancel_requested_for_missing_saga(scipio_cluster, cancel_saga):
    session, first_base_url, _, _ = scipio_cluster

    response = cancel_saga(session, first_base_url, uuid.uuid4().hex)

    assert response.status_code == 404
    assert response.json()["error"] == "saga not found"


def test_should_create_and_complete_saga_when_start_request_is_valid(scipio_cluster, start_saga, wait_for_status):
    # given
    session, first_base_url, _, _ = scipio_cluster

    # when
    saga_id = start_saga(session, first_base_url, "order_flow", {"user_id": 10})
    saga = wait_for_status(session, first_base_url, saga_id, "COMPLETED")

    # then
    assert saga["workflow"] == "order_flow"
    assert saga["context"]["user_id"] == 10


def test_should_return_canceling_saga_when_cancel_requested_and_compensation_is_not_implemented(scipio_cluster, start_saga, cancel_saga, wait_for_status):
    # given
    session, first_base_url, _, _ = scipio_cluster
    saga_id = start_saga(session, first_base_url, "cancel_flow", {"ref": "abc"})

    # when
    response = cancel_saga(session, first_base_url, saga_id)
    saga = wait_for_status(session, first_base_url, saga_id, "CANCELING")

    # then
    assert response.status_code == 500
    body = response.json()
    assert body["error"] == "saga compensation is not implemented"
    assert saga["status"] == "CANCELING"


def test_should_filter_sagas_when_status_query_is_provided(scipio_cluster, start_saga, wait_for_status, cancel_saga):
    # given
    session, first_base_url, _, _ = scipio_cluster
    first_saga_id = start_saga(session, first_base_url, "filter_flow", {"kind": "first"})
    second_saga_id = start_saga(session, first_base_url, "filter_flow", {"kind": "second"})

    # when
    cancel_response = cancel_saga(session, first_base_url, second_saga_id)
    assert cancel_response.status_code == 500
    wait_for_status(session, first_base_url, first_saga_id, "COMPLETED")
    response = session.get(f"{first_base_url}/sagas", params={"status": "CANCELING"}, timeout=3)

    # then
    assert response.status_code == 200
    ids = {item["id"] for item in response.json()["sagas"]}
    assert second_saga_id in ids
    assert first_saga_id not in ids


def test_should_share_saga_state_between_service_instances_when_using_postgres_store(
    scipio_cluster,
    start_saga,
    wait_for_status,
):
    # given
    session, first_base_url, second_base_url, _ = scipio_cluster

    # when
    saga_id = start_saga(session, first_base_url, "shared_flow", {"ref": "state"})
    saga = wait_for_status(session, second_base_url, saga_id, "COMPLETED")

    # then
    assert saga["id"] == saga_id


def test_should_dispatch_context_to_step_executor_when_saga_is_started_in_cluster(
    scipio_cluster,
    start_saga,
    wait_for_status,
    wait_for_step_dispatch,
):
    # given
    session, first_base_url, second_base_url, _ = scipio_cluster
    saga_context = {
        "user": {"id": 44, "tier": "gold"},
        "items": [{"sku": "book-1", "qty": 2}],
        "total": 120.75,
    }

    # when
    saga_id = start_saga(session, first_base_url, "dispatch_ctx_flow", saga_context)
    saga = wait_for_status(session, second_base_url, saga_id, "COMPLETED")
    calls = wait_for_step_dispatch(saga_id, expected_calls=1)

    # then
    assert saga["id"] == saga_id
    assert len(calls) == 1
    assert calls[0]["saga_id"] == saga_id
    assert calls[0]["workflow"] == "dispatch_ctx_flow"
    assert calls[0]["step_name"] == "dispatch_ctx_flow"
    assert calls[0]["context"] == saga_context


def test_should_handle_concurrent_cancellation_from_multiple_instances_when_lock_is_enabled(
    scipio_cluster,
    start_saga,
    wait_for_status,
    cancel_saga,
):
    # given
    session, first_base_url, second_base_url, _ = scipio_cluster
    saga_id = start_saga(session, first_base_url, "dual_cancel", {"ref": "same-saga"})
    
    # when
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(cancel_saga, session, first_base_url, saga_id),
            executor.submit(cancel_saga, session, second_base_url, saga_id),
        ]
        responses = [future.result().status_code for future in futures]
    saga = wait_for_status(session, first_base_url, saga_id, "CANCELING")

    # then
    assert responses == [500, 500]
    assert saga["status"] == "CANCELING"


def test_should_persist_saga_in_postgres_when_saga_is_started(
    scipio_cluster,
    start_saga,
    wait_for_status,
    db_transaction,
):
    # given
    session, first_base_url, _, _ = scipio_cluster

    # when
    saga_id = start_saga(session, first_base_url, "db_store_flow", {"user_id": 11, "channel": "testsuite"})
    wait_for_status(session, first_base_url, saga_id, "COMPLETED")
    db_transaction.execute(
        "SELECT id, workflow, status, context FROM sagas WHERE id = %s",
        (saga_id,),
    )
    row = db_transaction.fetchone()

    # then
    assert row is not None
    assert row["id"] == saga_id
    assert row["workflow"] == "db_store_flow"
    assert row["status"] == "COMPLETED"
    assert row["context"]["user_id"] == 11
    assert row["context"]["channel"] == "testsuite"


def test_should_persist_and_complete_saga_step_in_postgres_when_saga_is_started(
    scipio_cluster,
    start_saga,
    wait_for_status,
    db_transaction,
):
    # given
    session, first_base_url, _, _ = scipio_cluster
    saga_id = start_saga(session, first_base_url, "db_steps_flow", {"kind": "steps"})
    wait_for_status(session, first_base_url, saga_id, "COMPLETED")

    # when
    db_transaction.execute(
        "SELECT step_index, name, status, attempt, error FROM saga_steps WHERE saga_id = %s ORDER BY step_index ASC",
        (saga_id,),
    )
    rows = db_transaction.fetchall()

    # then
    assert len(rows) == 1
    assert rows[0]["step_index"] == 0
    assert rows[0]["name"] == "db_steps_flow"
    assert rows[0]["status"] == "COMPLETED"
    assert rows[0]["attempt"] == 1
    assert rows[0]["error"] is None


def test_should_recover_stale_running_step_when_worker_drains_postgres_steps(
    scipio_cluster,
    wait_for_status,
    db_connection,
):
    # given
    session, first_base_url, _, _ = scipio_cluster
    saga_id = uuid.uuid4().hex
    db_connection.execute(
        """
        INSERT INTO sagas (id, workflow, status, context, created_at, updated_at)
        VALUES (%s, %s, %s, %s::jsonb, NOW(), NOW() - INTERVAL '1 minute')
        """,
        (saga_id, "recover_flow", "RUNNING", "{}"),
    )
    db_connection.execute(
        """
        INSERT INTO saga_steps (saga_id, step_index, name, grpc_target, status, attempt, started_at, finished_at, error, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW() - INTERVAL '2 minute', NULL, NULL, NOW(), NOW() - INTERVAL '2 minute')
        """,
        (saga_id, 0, "recover_flow", "step-executor:50051", "RUNNING", 1),
    )

    # when
    saga = wait_for_status(session, first_base_url, saga_id, "COMPLETED")
    db_connection.execute(
        "SELECT status, attempt FROM saga_steps WHERE saga_id = %s AND step_index = 0",
        (saga_id,),
    )
    row = db_connection.fetchone()

    # then
    assert saga["status"] == "COMPLETED"
    assert row is not None
    assert row["status"] == "COMPLETED"
    assert row["attempt"] >= 2


def test_should_mark_saga_and_step_as_failed_when_step_target_is_unreachable(
    scipio_cluster,
    wait_for_status,
    db_connection,
):
    session, first_base_url, _, _ = scipio_cluster
    response = session.post(
        f"{first_base_url}/sagas",
        json={
            "workflow": "failed_step_flow",
            "context": {"reason": "unreachable"},
            "steps": [{"name": "failed_step_flow", "grpc_target": "unknown-step-target:50051"}],
        },
        timeout=3,
    )
    assert response.status_code == 202
    saga_id = response.json()["id"]

    saga = wait_for_status(session, first_base_url, saga_id, "FAILED")
    assert saga["status"] == "FAILED"
    assert len(saga["steps"]) == 1
    assert saga["steps"][0]["status"] == "FAILED"
    assert saga["steps"][0]["error"] is not None

    db_connection.execute(
        "SELECT status, error FROM saga_steps WHERE saga_id = %s AND step_index = 0",
        (saga_id,),
    )
    row = db_connection.fetchone()

    assert row is not None
    assert row["status"] == "FAILED"
    assert row["error"] is not None
