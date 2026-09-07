"""Constrained, exact-head GitHub review broker."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterator, Mapping, Protocol, TextIO


class BrokerError(ValueError):
    """A request was refused by a broker guard."""


class GitHubClient(Protocol):
    """Network adapter whose transport enforces ``timeout_seconds``."""

    def pull_request(
        self, repository: str, number: int, *, timeout_seconds: float
    ) -> Mapping[str, Any]: ...
    def required_checks(
        self, repository: str, sha: str, *, timeout_seconds: float
    ) -> Mapping[str, Mapping[str, str]]: ...
    def create_review(
        self,
        repository: str,
        number: int,
        *,
        event: str,
        body: str,
        timeout_seconds: float,
    ) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class ReviewRequest:
    caller: str
    repository: str
    pull_request: int
    head: str
    evidence_hash: str
    preflight_path: str
    preflight_hash: str
    preflight_digest: str
    decision: str
    body: str = ""
    request_id: str = ""


_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_ALLOWED_EVENTS = {"approve", "request-changes"}
_REQUIRED_PREFLIGHT = {
    "diff",
    "black",
    "ruff",
    "docs",
    "gitleaks",
    "shim-imports",
    "tests",
}
_TERMINAL_OUTCOMES = {"accepted", "rejected", "error", "uncertain"}


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class GitHubReviewBroker:
    """Submit only an exact-head review after all independent gates pass."""

    def __init__(
        self,
        client: GitHubClient,
        *,
        allowlisted_repositories: set[str],
        reviewer: str,
        audit_path: str | Path,
        timeout_seconds: float = 30.0,
    ):
        if not allowlisted_repositories:
            raise ValueError("repository allowlist must not be empty")
        if timeout_seconds <= 0:
            raise ValueError("timeout must be positive")
        self._client = client
        self._allowlist = frozenset(allowlisted_repositories)
        self._reviewer = reviewer
        self._audit_path = Path(audit_path)
        self._lock_path = self._audit_path.with_name(self._audit_path.name + ".lock")
        self._timeout = timeout_seconds

    @staticmethod
    def _serialize(record: Mapping[str, Any]) -> str:
        return _canonical_bytes(record).decode() + "\n"

    @contextmanager
    def _transaction(self) -> Iterator[TextIO]:
        """Hold one process-wide transaction through reservation and result."""
        self._audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock_path.open("a", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                with self._audit_path.open("a+", encoding="utf-8") as audit:
                    audit.seek(0)
                    for line in audit:
                        try:
                            json.loads(line)
                        except json.JSONDecodeError as exc:
                            raise BrokerError("corrupt audit log") from exc
                    yield audit
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _append(self, audit: TextIO, record: Mapping[str, Any]) -> None:
        line = self._serialize(record)
        json.loads(line)
        audit.seek(0, os.SEEK_END)
        audit.write(line)
        audit.flush()
        os.fsync(audit.fileno())

    @staticmethod
    def _prior_outcome(audit: TextIO, request_id: str) -> str | None:
        audit.seek(0)
        outcome = None
        for line in audit:
            record = json.loads(line)
            if record.get("request_id") != request_id:
                continue
            if record.get("type") == "github_review_reservation":
                outcome = "in_flight"
            elif record.get("type") == "github_review_result":
                value = str(record.get("outcome", ""))
                outcome = value if value in _TERMINAL_OUTCOMES else "uncertain"
        return outcome

    def _receipt(self, request: ReviewRequest) -> Mapping[str, Any]:
        path = Path(request.preflight_path)
        try:
            before = path.lstat()
            if not stat.S_ISREG(before.st_mode) or path.is_symlink():
                raise BrokerError("local preflight receipt is not a regular file")
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                raw = os.read(fd, 1024 * 1024 + 1)
                after = os.fstat(fd)
            finally:
                os.close(fd)
        except OSError as exc:
            raise BrokerError("local preflight receipt is unreadable") from exc
        if len(raw) > 1024 * 1024:
            raise BrokerError("local preflight receipt is too large")
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise BrokerError("local preflight receipt changed while reading")
        if hashlib.sha256(raw).hexdigest() != request.preflight_hash:
            raise BrokerError("local preflight receipt hash mismatch")
        try:
            receipt = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BrokerError("local preflight receipt is malformed") from exc
        if not isinstance(receipt, dict) or raw != _canonical_bytes(receipt) + b"\n":
            raise BrokerError("local preflight receipt is not canonical")
        digest = receipt.get("digest")
        unsigned = {key: value for key, value in receipt.items() if key != "digest"}
        if digest != hashlib.sha256(_canonical_bytes(unsigned)).hexdigest():
            raise BrokerError("local preflight canonical digest mismatch")
        if digest != request.preflight_digest:
            raise BrokerError("local preflight pinned digest mismatch")
        self._validate_receipt(request, receipt)
        return receipt

    @staticmethod
    def _validate_receipt(request: ReviewRequest, receipt: Mapping[str, Any]) -> None:
        required = {
            "schema",
            "repository",
            "base",
            "head",
            "tree",
            "paths",
            "diff_sha256",
            "checks",
            "state",
            "digest",
        }
        if (
            set(receipt) != required
            or receipt.get("schema") != "skfleet.local-ci-preflight/v1"
        ):
            raise BrokerError("local preflight receipt schema mismatch")
        if receipt.get("head") != request.head or receipt.get("state") != "PASS":
            raise BrokerError("local preflight candidate did not pass")
        for field in ("base", "head"):
            if not isinstance(receipt.get(field), str) or not _HEX40.fullmatch(
                receipt[field]
            ):
                raise BrokerError(f"local preflight {field} is malformed")
        if not isinstance(receipt.get("tree"), str) or not _HEX40.fullmatch(
            receipt["tree"]
        ):
            raise BrokerError("local preflight tree is malformed")
        for field in ("diff_sha256", "digest"):
            if not isinstance(receipt.get(field), str) or not _HEX64.fullmatch(
                receipt[field]
            ):
                raise BrokerError(f"local preflight {field} is malformed")
        repository = str(receipt.get("repository", "")).removesuffix(".git")
        if not repository.endswith("/" + request.repository):
            raise BrokerError("local preflight repository mismatch")
        paths = receipt.get("paths")
        if (
            not isinstance(paths, list)
            or not paths
            or any(
                not isinstance(path, str) or not path or path.startswith("/")
                for path in paths
            )
            or len(paths) != len(set(paths))
        ):
            raise BrokerError("local preflight paths are malformed")
        checks = receipt.get("checks")
        if not isinstance(checks, list):
            raise BrokerError("local preflight checks are malformed")
        names: set[str] = set()
        for check in checks:
            if not isinstance(check, dict) or set(check) != {
                "name",
                "exit_code",
                "elapsed_ms",
            }:
                raise BrokerError("local preflight check record is malformed")
            if not isinstance(check["name"], str) or check["name"] in names:
                raise BrokerError("local preflight check names are malformed")
            if check["exit_code"] != 0:
                raise BrokerError("local preflight check failed")
            if not isinstance(check["elapsed_ms"], int) or check["elapsed_ms"] < 0:
                raise BrokerError("local preflight check duration is malformed")
            names.add(check["name"])
        if names != _REQUIRED_PREFLIGHT:
            raise BrokerError("local preflight check set is incomplete")

    def _record(
        self,
        audit: TextIO,
        request: ReviewRequest,
        *,
        outcome: str,
        exit_status: int,
        response: Mapping[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        safe_response = {}
        if response:
            for key in ("id", "node_id", "user", "submitted_at", "html_url"):
                if key in response:
                    value = response[key]
                    safe_response[key] = (
                        value.get("login")
                        if key == "user" and isinstance(value, Mapping)
                        else value
                    )
        self._append(
            audit,
            {
                "type": "github_review_result",
                "request_id": request.request_id,
                "caller": request.caller,
                "repository": request.repository,
                "pull_request": request.pull_request,
                "head": request.head,
                "evidence_hash": request.evidence_hash,
                "preflight_hash": request.preflight_hash,
                "preflight_digest": request.preflight_digest,
                "decision": request.decision,
                "reviewer": self._reviewer,
                "github_review_identity": self._reviewer,
                "response": safe_response,
                "timestamp": time.time(),
                "exit_status": exit_status,
                "outcome": outcome,
                **({"error": error} if error else {}),
            },
        )

    def _github_call(self, method: Any, *args: Any, **kwargs: Any) -> Any:
        """Call synchronously; the injected transport owns the bounded deadline."""
        return method(*args, **kwargs, timeout_seconds=self._timeout)

    def submit_review(self, request: ReviewRequest) -> Mapping[str, Any]:
        """Reserve and submit one review without repeating uncertain side effects."""
        if not request.request_id:
            request = replace(request, request_id=str(uuid.uuid4()))
        self._validate(request)
        receipt = self._receipt(request)
        with self._transaction() as audit:
            prior = self._prior_outcome(audit, request.request_id)
            if prior:
                detail = (
                    "uncertain; reconcile GitHub before retry"
                    if prior in {"in_flight", "uncertain"}
                    else "terminal"
                )
                raise BrokerError(f"replayed request ({detail})")
            self._append(
                audit,
                {
                    "type": "github_review_reservation",
                    "state": "in_flight",
                    "request_id": request.request_id,
                    "caller": request.caller,
                    "repository": request.repository,
                    "pull_request": request.pull_request,
                    "head": request.head,
                    "evidence_hash": request.evidence_hash,
                    "preflight_hash": request.preflight_hash,
                    "preflight_digest": receipt["digest"],
                    "decision": request.decision,
                    "reviewer": self._reviewer,
                    "timestamp": time.time(),
                },
            )
            try:
                current = self._github_call(
                    self._client.pull_request, request.repository, request.pull_request
                )
                if str(current.get("head_sha", "")) != request.head:
                    raise BrokerError("head drift")
                if (
                    str(current.get("author", "")).casefold()
                    == self._reviewer.casefold()
                ):
                    raise BrokerError("same-author review")
                checks = self._github_call(
                    self._client.required_checks, request.repository, request.head
                )
                if not checks or any(
                    not isinstance(value, Mapping)
                    or str(value.get("status", "")).casefold() != "completed"
                    or str(value.get("conclusion", "")).casefold() != "success"
                    for value in checks.values()
                ):
                    raise BrokerError("missing or failed required checks")
                response = self._github_call(
                    self._client.create_review,
                    request.repository,
                    request.pull_request,
                    event=request.decision,
                    body=request.body,
                )
                self._record(
                    audit, request, outcome="accepted", exit_status=0, response=response
                )
                return {
                    key: response[key]
                    for key in ("id", "node_id", "html_url")
                    if key in response
                }
            except TimeoutError as exc:
                self._record(
                    audit,
                    request,
                    outcome="uncertain",
                    exit_status=124,
                    error="timeout",
                )
                raise BrokerError(
                    "GitHub request timed out; reconcile before retry"
                ) from exc
            except (BrokerError, ValueError) as exc:
                self._record(
                    audit, request, outcome="rejected", exit_status=1, error=str(exc)
                )
                raise
            except Exception as exc:
                self._record(
                    audit,
                    request,
                    outcome="uncertain",
                    exit_status=1,
                    error=type(exc).__name__,
                )
                raise BrokerError(
                    "GitHub result uncertain; reconcile before retry"
                ) from exc

    def _validate(self, request: ReviewRequest) -> None:
        if request.repository not in self._allowlist:
            raise BrokerError("repository is not allowlisted")
        if not isinstance(request.pull_request, int) or request.pull_request <= 0:
            raise BrokerError("malformed pull request")
        if not _HEX40.fullmatch(request.head):
            raise BrokerError("malformed exact head")
        for value, message in (
            (request.evidence_hash, "missing or malformed evidence hash"),
            (request.preflight_hash, "missing or malformed local preflight hash"),
            (request.preflight_digest, "missing or malformed local preflight digest"),
        ):
            if not _HEX64.fullmatch(value):
                raise BrokerError(message)
        if request.decision not in _ALLOWED_EVENTS:
            raise BrokerError("unsupported review decision")
        if len(request.body) > 10000:
            raise BrokerError("review body too large")
        if request.caller.casefold() == self._reviewer.casefold():
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
