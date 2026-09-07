"""Constrained, exact-head GitHub review broker.

The broker is deliberately an adapter around a narrow GitHub client.  Callers
can submit a review, but never supply (or receive) GitHub credentials or an
arbitrary API operation.  Audit records are append-only JSON lines and are
validated before they are written.
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol


class BrokerError(ValueError):
    """A request was refused by a broker guard."""


class GitHubClient(Protocol):
    def pull_request(self, repository: str, number: int) -> Mapping[str, Any]: ...
    def required_checks(self, repository: str, sha: str) -> Mapping[str, str]: ...
    def create_review(self, repository: str, number: int, *, event: str, body: str) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class ReviewRequest:
    caller: str
    repository: str
    pull_request: int
    head: str
    evidence_hash: str
    decision: str  # approve or request-changes
    body: str = ""
    request_id: str = ""


_HEX40 = re.compile(r"^[0-9a-fA-F]{40}$")
_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
_ALLOWED_EVENTS = {"approve", "request-changes"}
_TERMINAL_SUCCESS = {"success", "completed", "passed"}


class GitHubReviewBroker:
    """Submit only an exact-head review after all independent gates pass."""

    def __init__(self, client: GitHubClient, *, allowlisted_repositories: set[str],
                 reviewer: str, audit_path: str | Path, timeout_seconds: float = 30.0):
        if not allowlisted_repositories:
            raise ValueError("repository allowlist must not be empty")
        self._client = client
        self._allowlist = frozenset(allowlisted_repositories)
        self._reviewer = reviewer
        self._audit_path = Path(audit_path)
        self._timeout = timeout_seconds
        self._lock = threading.Lock()

    @staticmethod
    def _serialize(record: Mapping[str, Any]) -> str:
        # A single canonical serializer prevents hand-built JSON and secret
        # leakage through accidental values.
        return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"

    def _audit(self, record: Mapping[str, Any]) -> None:
        line = self._serialize(record)
        json.loads(line)  # validate every line before appending
        self._audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self._audit_path.open("a+", encoding="utf-8") as stream:
            stream.seek(0)
            for prior in stream:
                json.loads(prior)
            stream.seek(0, 2)
            stream.write(line)
            stream.flush()

    def _record(self, request: ReviewRequest, *, outcome: str, exit_status: int,
                response: Mapping[str, Any] | None = None, error: str | None = None) -> None:
        safe_response = {}
        if response:
            # GitHub response fields are intentionally allowlisted.
            for key in ("id", "node_id", "user", "submitted_at", "html_url"):
                if key in response:
                    value = response[key]
                    safe_response[key] = value.get("login") if key == "user" and isinstance(value, Mapping) else value
        self._audit({
            "type": "github_review_result", "request_id": request.request_id,
            "caller": request.caller, "repository": request.repository,
            "pull_request": request.pull_request, "head": request.head,
            "evidence_hash": request.evidence_hash, "decision": request.decision,
            "reviewer": self._reviewer, "github_review_identity": self._reviewer,
            "response": safe_response, "timestamp": time.time(),
            "exit_status": exit_status, "outcome": outcome,
            **({"error": error} if error else {}),
        })

    def submit_review(self, request: ReviewRequest) -> Mapping[str, Any]:
        """Validate and submit one review; failures are audited and raised."""
        if not request.request_id:
            request = ReviewRequest(**{**request.__dict__, "request_id": str(uuid.uuid4())})
        event = {"type": "github_review_request", "request_id": request.request_id,
                 "caller": request.caller, "repository": request.repository,
                 "pull_request": request.pull_request, "head": request.head,
                 "evidence_hash": request.evidence_hash, "decision": request.decision,
                 "reviewer": self._reviewer, "timestamp": time.time()}
        with self._lock:
            try:
                # Request IDs are idempotency keys.  Scan the append-only log
                # before accepting a second submission, while still recording
                # the replay attempt as its own request event.
                self._audit(event)
                if self._seen_request(request.request_id):
                    raise BrokerError("replayed request")
                self._validate(request)
                current = self._client.pull_request(request.repository, request.pull_request)
                if str(current.get("head_sha", "")) != request.head:
                    raise BrokerError("head drift")
                if str(current.get("author", "")) == self._reviewer:
                    raise BrokerError("same-author review")
                checks = self._client.required_checks(request.repository, request.head)
                if not checks or any(str(v).lower() not in _TERMINAL_SUCCESS for v in checks.values()):
                    raise BrokerError("missing or failed required checks")
                response = self._client.create_review(request.repository, request.pull_request,
                                                      event=request.decision, body=request.body)
                self._record(request, outcome="accepted", exit_status=0, response=response)
                return {k: response[k] for k in ("id", "node_id", "html_url") if k in response}
            except TimeoutError as exc:
                self._record(request, outcome="timeout", exit_status=124, error="timeout")
                raise BrokerError("GitHub request timed out") from exc
            except (BrokerError, ValueError) as exc:
                self._record(request, outcome="rejected", exit_status=1, error=str(exc))
                raise
            except Exception as exc:
                self._record(request, outcome="error", exit_status=1, error=type(exc).__name__)
                raise BrokerError("GitHub broker failure") from exc

    def _seen_request(self, request_id: str) -> bool:
        if not self._audit_path.exists():
            return False
        with self._audit_path.open("r", encoding="utf-8") as stream:
            for line in stream:
                try:
                    if json.loads(line).get("request_id") == request_id and json.loads(line).get("type") == "github_review_result":
                        return True
                except json.JSONDecodeError as exc:
                    raise BrokerError("corrupt audit log") from exc
        return False

    def _validate(self, request: ReviewRequest) -> None:
        if request.repository not in self._allowlist:
            raise BrokerError("repository is not allowlisted")
        if not isinstance(request.pull_request, int) or request.pull_request <= 0:
            raise BrokerError("malformed pull request")
        if not _HEX40.fullmatch(request.head):
            raise BrokerError("malformed exact head")
        if not _HEX64.fullmatch(request.evidence_hash):
            raise BrokerError("missing or malformed evidence hash")
        if request.decision not in _ALLOWED_EVENTS:
            raise BrokerError("unsupported review decision")
        if len(request.body) > 10000:
            raise BrokerError("review body too large")
        if request.caller == self._reviewer:
            raise BrokerError("same-author review")
        if any(word in request.body.lower() for word in ("merge", "push", "deploy")):
            raise BrokerError("merge/push requests are forbidden")


def evidence_sha256(path: str | Path) -> str:
    """Hash evidence bytes without exposing them to the broker or GitHub."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
