"""Phase 11.0: which Databricks Foundation Model chat endpoints can this workspace call?"""

from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

PREFERENCE = ["claude", "llama-3-3-70b", "llama-4", "gpt-oss-120b", "gpt-oss", "llama"]


def main() -> None:
    w = WorkspaceClient(profile="velo")
    endpoints = list(w.serving_endpoints.list())
    print(f"=== {len(endpoints)} serving endpoints ===")
    for e in endpoints:
        print(f"  {e.name:<55} task={e.task}")

    chat = [e.name for e in endpoints if (e.task or "").startswith("llm/v1/chat")]
    ranked = sorted(
        chat,
        key=lambda n: next(
            (i for i, p in enumerate(PREFERENCE) if p in n), len(PREFERENCE)
        ),
    )
    print(f"\n=== Testing up to 4 chat endpoints (of {len(chat)}) ===")
    for name in ranked[:4]:
        try:
            resp = w.serving_endpoints.query(
                name=name,
                messages=[
                    ChatMessage(
                        role=ChatMessageRole.USER, content="Reply with exactly: OK"
                    )
                ],
                max_tokens=10,
            )
            print(f"  WORKS  {name}: {resp.choices[0].message.content!r}")
        except DatabricksError as exc:
            print(f"  FAILS  {name}: {str(exc)[:120]}")


if __name__ == "__main__":
    main()
