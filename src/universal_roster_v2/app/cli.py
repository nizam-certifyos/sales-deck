"""Optional CLI wrapper for standalone Universal Roster V2 (web-first product)."""

from __future__ import annotations

import argparse
import json
from typing import List

from universal_roster_v2.core.session import UniversalRosterSession


def _print(payload) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Standalone Universal Roster V2")
    parser.add_argument("--workspace", default=None)
    parser.add_argument("--tenant-id", default="default-tenant")
    parser.add_argument("--client-id", default="default-client")
    parser.add_argument("--thread-id", default="default")
    parser.add_argument("--file", default=None)
    parser.add_argument("--type", choices=["practitioner", "facility"], default=None)
    parser.add_argument("--profile-full-roster-learning", action="store_true")
    parser.add_argument("--profile-max-rows", type=int, default=None)
    parser.add_argument("--note", action="append", default=[])
    parser.add_argument("--suggest", action="store_true")
    parser.add_argument("--generate", choices=["schema", "processor", "main", "full"], default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--run-input", default=None)
    parser.add_argument("--generated-dir", default=None)
    parser.add_argument("--export-training", action="store_true")
    parser.add_argument("--train", action="store_true")
    parser.add_argument("--train-extra-arg", action="append", default=[])

    args = parser.parse_args(argv)

    session = UniversalRosterSession(
        workspace_dir=args.workspace,
        workspace_scope={
            "tenant_id": args.tenant_id,
            "client_id": args.client_id,
            "thread_id": args.thread_id,
            "workspace_path": args.workspace,
        },
    )

    if args.note:
        session.update_instructions_context({"free_text_notes": [note for note in args.note if str(note).strip()]})

    if args.file:
        session.load_file(
            args.file,
            roster_type=args.type,
            profile_full_roster_learning=args.profile_full_roster_learning,
            profile_max_rows=args.profile_max_rows,
        )

    if args.suggest:
        session.suggest(use_llm_for_unresolved=True)

    if args.generate:
        result = session.generate(mode=args.generate, output_dir=args.output_dir)
        _print(result)
        return 0

    if args.run:
        if not args.run_input:
            raise SystemExit("--run requires --run-input")
        result = session.run_generated_pipeline(
            input_file=args.run_input,
            generated_dir=args.generated_dir,
            output_dir=args.output_dir,
        )
        _print(result)
        return 0

    if args.export_training:
        result = session.export_training_datasets(output_dir=args.output_dir)
        _print(result)
        return 0

    if args.train:
        result = session.run_trainer(
            export_dir=args.output_dir,
            extra_args=args.train_extra_arg,
        )
        _print(result)
        return 0

    _print(session.status())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
