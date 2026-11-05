#!/usr/bin/env python3
"""Run trigger evaluation for a skill description.

Tests whether a skill's description causes an AI agent to trigger (read the
skill) for a set of queries. Supports both Claude Code (via `claude -p` CLI)
and Cursor (via LLM simulation). Outputs results as JSON.
"""

import argparse
import json
import os
import select
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from scripts.utils import detect_platform, parse_skill_md


def find_project_root() -> Path:
    """Find the project root by walking up from cwd looking for config dirs.

    Checks for .claude/ and .cursor/ directories, mimicking how both
    Claude Code and Cursor discover their project root.
    """
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / ".claude").is_dir() or (parent / ".cursor").is_dir():
            return parent
    return current


def _run_query_cursor(
    query: str,
    skill_name: str,
    skill_description: str,
    model: str | None = None,
) -> bool:
    """Test skill triggering via LLM simulation (for Cursor).

    Since Cursor has no CLI equivalent to `claude -p`, we simulate triggering
    by asking a model whether it would invoke the skill for the given query.
    This tests description quality rather than actual runtime behavior, but is
    directionally accurate for A/B testing descriptions.
    """
    import anthropic

    system_prompt = (
        "You are a coding assistant with access to skills. Available skills:\n"
        f"- {skill_name}: {skill_description}\n\n"
        f'Given the following user query, would you invoke the "{skill_name}" skill? '
        "Reply with ONLY \"YES\" or \"NO\"."
    )

    client = anthropic.Anthropic()
    response = client.messages.create(
        model=model or "claude-sonnet-4-6",
        max_tokens=5,
        system=system_prompt,
        messages=[{"role": "user", "content": query}],
    )
    text = response.content[0].text.strip().upper() if response.content else ""

    return "YES" in text


def _run_query_claude(
    query: str,
    skill_name: str,
    skill_description: str,
    timeout: int,
    project_root: str,
    model: str | None = None,
) -> bool:
    """Run a single query against Claude Code CLI and return whether the skill was triggered.

    Tests the real skill in .claude/skills/ by running `claude -p` and watching
    for ToolSearch/Skill/Read tool calls that reference the skill name.

    Claude Code's modern flow is: ToolSearch -> Skill tool. The older flow
    used Read to load command files directly. Both are detected.
    """
    cmd = [
        "claude",
        "-p", query,
        "--output-format", "stream-json",
        "--verbose",
        "--include-partial-messages",
    ]
    if model:
        cmd.extend(["--model", model])

    # Remove CLAUDECODE env var to allow nesting claude -p inside a
    # Claude Code session. The guard is for interactive terminal conflicts;
    # programmatic subprocess usage is safe.
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        cwd=project_root,
        env=env,
    )

    triggered = False
    start_time = time.time()
    buffer = ""
    # Track state for stream event detection
    pending_tool_name = None
    accumulated_json = ""
    # Track the multi-turn flow: ToolSearch("select:Skill") -> Skill("skill-name")
    # The first ToolSearch loads the Skill tool, then the Skill tool invokes the skill.
    seen_skill_tool_loaded = False
    first_tool_seen = False

    # Tools that are part of the skill invocation flow
    skill_tools = {"Skill", "Read", "ToolSearch"}

    try:
        while time.time() - start_time < timeout:
            if process.poll() is not None:
                remaining = process.stdout.read()
                if remaining:
                    buffer += remaining.decode("utf-8", errors="replace")
                break

            ready, _, _ = select.select([process.stdout], [], [], 1.0)
            if not ready:
                continue

            chunk = os.read(process.stdout.fileno(), 8192)
            if not chunk:
                break
            buffer += chunk.decode("utf-8", errors="replace")

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Early detection via stream events
                if event.get("type") == "stream_event":
                    se = event.get("event", {})
                    se_type = se.get("type", "")

                    if se_type == "content_block_start":
                        cb = se.get("content_block", {})
                        if cb.get("type") == "tool_use":
                            tool_name = cb.get("name", "")
                            if tool_name in skill_tools:
                                pending_tool_name = tool_name
                                accumulated_json = ""
                            elif not first_tool_seen and not seen_skill_tool_loaded:
                                # Very first tool call is unrelated to skills
                                return False

                            first_tool_seen = True

                    elif se_type == "content_block_delta" and pending_tool_name:
                        delta = se.get("delta", {})
                        if delta.get("type") == "input_json_delta":
                            accumulated_json += delta.get("partial_json", "")

                    elif se_type == "content_block_stop":
                        if pending_tool_name:
                            if pending_tool_name == "ToolSearch":
                                # ToolSearch("select:Skill") loads the Skill tool
                                if "Skill" in accumulated_json:
                                    seen_skill_tool_loaded = True
                            elif pending_tool_name == "Skill":
                                # Skill("executive-assistant-setup") invokes the skill
                                if skill_name in accumulated_json:
                                    return True
                            elif pending_tool_name == "Read":
                                if skill_name in accumulated_json:
                                    return True
                            pending_tool_name = None
                            accumulated_json = ""

                    # Don't bail on message_stop -- conversation continues
                    # across multiple turns (ToolSearch -> user result -> Skill)

                # Fallback: full assistant message
                elif event.get("type") == "assistant":
                    message = event.get("message", {})
                    for content_item in message.get("content", []):
                        if content_item.get("type") != "tool_use":
                            continue
                        tool_name = content_item.get("name", "")
                        tool_input = content_item.get("input", {})
                        if tool_name == "ToolSearch":
                            if "Skill" in json.dumps(tool_input):
                                seen_skill_tool_loaded = True
                        elif tool_name == "Skill" and skill_name in tool_input.get("skill", ""):
                            return True
                        elif tool_name == "Read" and skill_name in tool_input.get("file_path", ""):
                            return True

                elif event.get("type") == "result":
                    return triggered
    finally:
        # Clean up process on any exit path (return, exception, timeout)
        if process.poll() is None:
            process.kill()
            process.wait()

    return triggered


def run_single_query(
    query: str,
    skill_name: str,
    skill_description: str,
    timeout: int,
    project_root: str,
    model: str | None = None,
    platform: str = "claude",
) -> bool:
    """Dispatch to the appropriate backend based on platform."""
    if platform == "cursor":
        return _run_query_cursor(query, skill_name, skill_description, model)
    return _run_query_claude(query, skill_name, skill_description, timeout, project_root, model)


def run_eval(
    eval_set: list[dict],
    skill_name: str,
    description: str,
    num_workers: int,
    timeout: int,
    project_root: Path,
    runs_per_query: int = 1,
    trigger_threshold: float = 0.5,
    model: str | None = None,
    platform: str = "claude",
) -> dict:
    """Run the full eval set and return results."""
    results = []

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_info = {}
        for item in eval_set:
            for run_idx in range(runs_per_query):
                future = executor.submit(
                    run_single_query,
                    item["query"],
                    skill_name,
                    description,
                    timeout,
                    str(project_root),
                    model,
                    platform,
                )
                future_to_info[future] = (item, run_idx)

        query_triggers: dict[str, list[bool]] = {}
        query_items: dict[str, dict] = {}
        for future in as_completed(future_to_info):
            item, _ = future_to_info[future]
            query = item["query"]
            query_items[query] = item
            if query not in query_triggers:
                query_triggers[query] = []
            try:
                query_triggers[query].append(future.result())
            except Exception as e:
                print(f"Warning: query failed: {e}", file=sys.stderr)
                query_triggers[query].append(False)

    for query, triggers in query_triggers.items():
        item = query_items[query]
        trigger_rate = sum(triggers) / len(triggers)
        should_trigger = item["should_trigger"]
        if should_trigger:
            did_pass = trigger_rate >= trigger_threshold
        else:
            did_pass = trigger_rate < trigger_threshold
        results.append({
            "query": query,
            "should_trigger": should_trigger,
            "trigger_rate": trigger_rate,
            "triggers": sum(triggers),
            "runs": len(triggers),
            "pass": did_pass,
        })

    passed = sum(1 for r in results if r["pass"])
    total = len(results)

    return {
        "skill_name": skill_name,
        "description": description,
        "results": results,
        "summary": {
            "total": total,
            "passed": passed,
            "failed": total - passed,
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Run trigger evaluation for a skill description")
    parser.add_argument("--eval-set", required=True, help="Path to eval set JSON file")
    parser.add_argument("--skill-path", required=True, help="Path to skill directory")
    parser.add_argument("--description", default=None, help="Override description to test")
    parser.add_argument("--num-workers", type=int, default=10, help="Number of parallel workers")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout per query in seconds")
    parser.add_argument("--runs-per-query", type=int, default=3, help="Number of runs per query")
    parser.add_argument("--trigger-threshold", type=float, default=0.5, help="Trigger rate threshold")
    parser.add_argument("--model", default=None, help="Model to use (default: claude-sonnet-4-6)")
    parser.add_argument("--platform", default=None, choices=["claude", "cursor"], help="Target platform (default: auto-detect)")
    parser.add_argument("--verbose", action="store_true", help="Print progress to stderr")
    args = parser.parse_args()

    platform = args.platform or detect_platform()

    eval_set = json.loads(Path(args.eval_set).read_text())
    skill_path = Path(args.skill_path)

    if not (skill_path / "SKILL.md").exists():
        print(f"Error: No SKILL.md found at {skill_path}", file=sys.stderr)
        sys.exit(1)

    parsed = parse_skill_md(skill_path)
    name, original_description = parsed["name"], parsed["description"]
    description = args.description or original_description
    project_root = find_project_root()

    if args.verbose:
        print(f"Platform: {platform}", file=sys.stderr)
        print(f"Evaluating: {description}", file=sys.stderr)

    output = run_eval(
        eval_set=eval_set,
        skill_name=name,
        description=description,
        num_workers=args.num_workers,
        timeout=args.timeout,
        project_root=project_root,
        runs_per_query=args.runs_per_query,
        trigger_threshold=args.trigger_threshold,
        model=args.model,
        platform=platform,
    )

    if args.verbose:
        summary = output["summary"]
        print(f"Results: {summary['passed']}/{summary['total']} passed", file=sys.stderr)
        for r in output["results"]:
            status = "PASS" if r["pass"] else "FAIL"
            rate_str = f"{r['triggers']}/{r['runs']}"
            print(f"  [{status}] rate={rate_str} expected={r['should_trigger']}: {r['query'][:70]}", file=sys.stderr)

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
