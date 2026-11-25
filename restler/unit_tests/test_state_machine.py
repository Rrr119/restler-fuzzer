# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import os
from pathlib import Path

import pytest

from engine.core import state_machine


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

OPENAPI_SPECS = [
    REPOSITORY_ROOT / "demo_server" / "swagger.json",
    REPOSITORY_ROOT
    / "src"
    / "compiler"
    / "Restler.Compiler.Test"
    / "swagger"
    / "dependencyTests"
    / "input_producer_spec.json",
    REPOSITORY_ROOT
    / "src"
    / "compiler"
    / "Restler.Compiler.Test"
    / "swagger"
    / "dependencyTests"
    / "nested_objects_naming.json",
]


def _build_plan(path, max_depth=12):
    operations = state_machine.operations_from_openapi(str(path))
    machine = state_machine.build_state_machine(operations)
    return state_machine.generate_stateful_sequences(
        machine, max_depth=max_depth, random_seed=12345
    )


@pytest.mark.parametrize("spec_path", OPENAPI_SPECS)
def test_stateful_plan_improves_state_coverage(spec_path):
    plan = _build_plan(spec_path)
    report = state_machine.state_coverage_report(plan)

    assert report["stateful"]["transition_coverage"] == 1.0
    assert report["improvement"]["state_coverage_delta"] > 0
    assert report["improvement"]["transition_coverage_delta"] > 0
    assert report["random_baseline"]["transition_coverage"] < 1.0


def test_plan_is_deterministic_and_replayable():
    spec_path = OPENAPI_SPECS[0]
    first_plan = _build_plan(spec_path)
    second_plan = _build_plan(spec_path)

    assert first_plan.plan_id == second_plan.plan_id
    assert first_plan.to_json() == second_plan.to_json()

    loaded = state_machine.StatefulSequencePlan.from_dict(
        json.loads(first_plan.to_json())
    )
    assert loaded.plan_id == first_plan.plan_id
    assert loaded.to_json() == first_plan.to_json()
    loaded_report = state_machine.state_coverage_report(loaded)
    first_report = state_machine.state_coverage_report(first_plan)
    for key in ("model", "stateful", "random_baseline", "improvement"):
        assert loaded_report[key] == first_report[key]

    _, replay_coverage = state_machine.measure_sequences(
        loaded.machine,
        [sequence.operation_ids for sequence in loaded.sequences],
    )
    assert replay_coverage == first_plan.coverage


def test_compiler_grammar_has_exact_dependencies_and_state_coverage():
    grammar_path = (
        REPOSITORY_ROOT
        / "restler"
        / "unit_tests"
        / "grammar_schema_test_files"
        / "demo_server_grammar.json"
    )
    operations = state_machine.operations_from_grammar(str(grammar_path))
    machine = state_machine.build_state_machine(operations)
    plan = state_machine.generate_stateful_sequences(machine, max_depth=10)

    post = next(
        operation
        for operation in operations
        if operation.method == "POST"
        and operation.endpoint == "/api/blog/posts"
    )
    get_by_id = next(
        operation
        for operation in operations
        if operation.method == "GET"
        and operation.endpoint == "/api/blog/posts/{postId}"
    )
    delete_by_id = next(
        operation
        for operation in operations
        if operation.method == "DELETE"
        and operation.endpoint == "/api/blog/posts/{postId}"
    )

    assert "_api_blog_posts_post_id" in post.produces
    assert "_api_blog_posts_post_id" in get_by_id.consumes
    assert "_api_blog_posts_post_id" in delete_by_id.removes
    assert plan.coverage["transition_coverage"] == 1.0


def test_plan_file_is_canonical_json(tmp_path):
    plan = _build_plan(OPENAPI_SPECS[0])
    output_path = tmp_path / "state_sequences.json"
    plan.write(str(output_path))

    contents = output_path.read_text(encoding="utf-8")
    parsed = json.loads(contents)
    assert parsed["schema_version"] == state_machine.PLAN_SCHEMA_VERSION
    assert parsed["generator"] == state_machine.GENERATOR_NAME
    assert contents.endswith("\n")

