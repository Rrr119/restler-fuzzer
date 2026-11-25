# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Generate and measure state-machine driven RESTler sequences.

Examples:

    python utilities/stateful_sequence_generation.py \
        --spec api/openapi.json --output-dir stateful

    python utilities/stateful_sequence_generation.py \
        --grammar Compile/grammar.json --max-depth 20
"""
from __future__ import print_function

import argparse
import json
import os
import re
import sys

REPOSITORY_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENGINE_ROOT = os.path.join(REPOSITORY_ROOT, "restler")
if ENGINE_ROOT not in sys.path:
    sys.path.insert(0, ENGINE_ROOT)

from engine.core import state_machine  # noqa: E402


def _slug(value):
    value = os.path.splitext(os.path.basename(value))[0]
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def _build_plan(path, is_grammar, args):
    if is_grammar:
        operations = state_machine.operations_from_grammar(path)
    else:
        operations = state_machine.operations_from_openapi(path)
    machine = state_machine.build_state_machine(operations)
    plan = state_machine.generate_stateful_sequences(
        machine,
        max_states=args.max_states,
        max_depth=args.max_depth,
        max_sequences=args.max_sequences,
        baseline_max_depth=args.baseline_depth,
        random_seed=args.seed,
    )
    return machine, plan


def _write_plan(output_dir, name, machine, plan):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plan_path = os.path.join(output_dir, name + ".state_sequences.json")
    model_path = os.path.join(output_dir, name + ".state_machine.json")
    coverage_path = os.path.join(output_dir, name + ".state_coverage.json")
    plan.write(plan_path)
    state_machine._write_json(
        model_path,
        machine.state_machine_dict(getattr(plan, "exploration", None)),
        indent=2
    )
    state_machine._write_json(
        coverage_path, state_machine.state_coverage_report(plan), indent=2
    )
    return plan_path, coverage_path


def main():
    parser = argparse.ArgumentParser(
        description="Generate deterministic state-machine driven RESTler sequences."
    )
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument(
        "--spec",
        action="append",
        help="OpenAPI JSON/YAML document. May be specified more than once.",
    )
    inputs.add_argument(
        "--grammar",
        action="append",
        help="RESTler compiler-generated grammar.json. May be specified more than once.",
    )
    parser.add_argument(
        "--output-dir",
        default="stateful_sequences",
        help="Directory for plans and coverage reports (default: stateful_sequences).",
    )
    parser.add_argument("--max-states", type=int, default=10000)
    parser.add_argument("--max-depth", type=int, default=100)
    parser.add_argument("--max-sequences", type=int, default=None)
    parser.add_argument("--baseline-depth", type=int, default=2)
    parser.add_argument("--seed", type=int, default=12345)
    args = parser.parse_args()

    paths = args.grammar if args.grammar else args.spec
    is_grammar = bool(args.grammar)
    summary = []
    for path in paths:
        machine, plan = _build_plan(path, is_grammar, args)
        plan_path, coverage_path = _write_plan(
            args.output_dir, _slug(path), machine, plan
        )
        report = state_machine.state_coverage_report(plan)
        summary.append(
            {
                "input": path,
                "sequences": len(plan.sequences),
                "states": report["model"]["states_reachable"],
                "transitions": report["model"]["transitions_reachable"],
                "stateful_state_coverage": report["stateful"]["state_coverage"],
                "stateful_transition_coverage": report["stateful"][
                    "transition_coverage"
                ],
                "random_state_coverage": report["random_baseline"].get(
                    "state_coverage", 0.0
                ),
                "random_transition_coverage": report["random_baseline"].get(
                    "transition_coverage", 0.0
                ),
                "state_coverage_delta": report["improvement"][
                    "state_coverage_delta"
                ],
                "transition_coverage_delta": report["improvement"][
                    "transition_coverage_delta"
                ],
                "plan": plan_path,
                "coverage": coverage_path,
            }
        )

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

