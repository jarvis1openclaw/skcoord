import hashlib
import json
import multiprocessing
import time

import pytest

from skcoord.github_review_broker import BrokerError, GitHubReviewBroker, ReviewRequest

HEAD, EVIDENCE = "a" * 40, "b" * 64
CHECKS = ("diff", "black", "ruff", "docs", "gitleaks", "shim-imports", "tests")


class Client:
    def __init__(self, head=HEAD, checks=None, author="contributor"):
        self.head = head
        self.checks = checks or {
            "tests": {"status": "completed", "conclusion": "success"}
        }
        self.author = author
        self.calls = []

    def pull_request(self, repository, number, *, timeout_seconds):
        assert timeout_seconds > 0
        return {"head_sha": self.head, "author": self.author}

    def required_checks(self, repository, sha, *, timeout_seconds):
        assert timeout_seconds > 0
        return self.checks

    def create_review(self, repository, number, *, event, body, timeout_seconds):
        assert timeout_seconds > 0
        self.calls.append((repository, number, event, body))
        return {
            "id": 7,
            "node_id": "n7",
            "html_url": "https://example/review/7",
            "token": "secret",
        }


def receipt(tmp_path, **changes):
    tmp_path.mkdir(parents=True, exist_ok=True)
    data = {
        "schema": "skfleet.local-ci-preflight/v1",
        "repository": "https://github.com/org/repo.git",
        "base": "c" * 40,
        "head": HEAD,
        "tree": "d" * 40,
        "paths": ["src/change.py"],
        "diff_sha256": "e" * 64,
        "checks": [{"name": name, "exit_code": 0, "elapsed_ms": 1} for name in CHECKS],
        "state": "PASS",
    }
    data.update(changes)
    data["digest"] = hashlib.sha256(
        json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    raw = json.dumps(data, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    path = tmp_path / "receipt.json"
    path.write_bytes(raw)
    return path, hashlib.sha256(raw).hexdigest(), data["digest"]


def req(tmp_path, **kw):
    path, file_hash, digest = receipt(tmp_path)
    data = dict(
        caller="link",
        repository="org/repo",
        pull_request=4,
        head=HEAD,
        evidence_hash=EVIDENCE,
        preflight_path=str(path),
        preflight_hash=file_hash,
        preflight_digest=digest,
        decision="approve",
        request_id="r1",
    )
    data.update(kw)
    return ReviewRequest(**data)


def broker(tmp_path, client=None):
    return GitHubReviewBroker(
        client or Client(),
        allowlisted_repositories={"org/repo"},
        reviewer="owner",
        audit_path=tmp_path / "audit.jsonl",
    )


def test_success_reserves_then_records_terminal_and_redacts(tmp_path):
    client = Client()
    assert broker(tmp_path, client).submit_review(req(tmp_path))["id"] == 7
    records = [
        json.loads(line) for line in (tmp_path / "audit.jsonl").read_text().splitlines()
    ]
    assert [records[0]["state"], records[1]["outcome"]] == ["in_flight", "accepted"]
    assert (
        len(client.calls) == 1
        and "secret" not in (tmp_path / "audit.jsonl").read_text()
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"state": "FAIL"},
        {"head": "f" * 40},
        {"tree": "bad"},
        {"paths": []},
        {"diff_sha256": "bad"},
        {
            "checks": [
                {"name": name, "exit_code": 1, "elapsed_ms": 1} for name in CHECKS
            ]
        },
    ],
)
def test_receipt_semantics_fail_closed(tmp_path, changes):
    path, file_hash, digest = receipt(tmp_path, **changes)
    request = req(
        tmp_path / "req",
        preflight_path=str(path),
        preflight_hash=file_hash,
        preflight_digest=digest,
    )
    with pytest.raises(BrokerError):
        broker(tmp_path).submit_review(request)


def test_receipt_bytes_hash_and_canonical_digest_are_verified(tmp_path):
    request = req(tmp_path)
    with open(request.preflight_path, "ab") as stream:
        stream.write(b" ")
    with pytest.raises(BrokerError, match="hash mismatch"):
        broker(tmp_path).submit_review(request)
    request = req(tmp_path / "digest")
    with pytest.raises(BrokerError, match="pinned digest"):
        broker(tmp_path / "digest").submit_review(
            ReviewRequest(**{**request.__dict__, "preflight_digest": "f" * 64})
        )


def test_rejections_and_casefolded_identities(tmp_path):
    for index, bad in enumerate(
        (
            dict(repository="other/x"),
            dict(evidence_hash="x" * 64),
            dict(preflight_hash="bad"),
            dict(decision="merge"),
            dict(body="please push"),
            dict(caller="OWNER"),
        )
    ):
        with pytest.raises(BrokerError):
            broker(tmp_path / str(index)).submit_review(
                req(tmp_path / str(index), **bad)
            )
    with pytest.raises(BrokerError, match="same-author"):
        broker(tmp_path / "author", Client(author="OwNeR")).submit_review(
            req(tmp_path / "author")
        )


@pytest.mark.parametrize(
    "check",
    [
        {"status": "completed", "conclusion": "failure"},
        {"status": "in_progress", "conclusion": "success"},
        {"unit": "completed"},
        "success",
    ],
)
def test_required_checks_need_completed_and_success(tmp_path, check):
    with pytest.raises(BrokerError, match="required checks"):
        broker(tmp_path, Client(checks={"tests": check})).submit_review(req(tmp_path))


def test_replay_is_terminal(tmp_path):
    request = req(tmp_path)
    instance = broker(tmp_path)
    instance.submit_review(request)
    with pytest.raises(BrokerError, match="replayed"):
        instance.submit_review(request)


def test_timeout_is_transport_owned_audited_and_never_retried(tmp_path):
    class Timeout(Client):
        def create_review(self, *args, **kwargs):
            raise TimeoutError

    request, client = req(tmp_path), Timeout()
    with pytest.raises(BrokerError, match="reconcile"):
        broker(tmp_path, client).submit_review(request)
    assert '"outcome":"uncertain"' in (tmp_path / "audit.jsonl").read_text()
    with pytest.raises(BrokerError, match="replayed"):
        broker(tmp_path, client).submit_review(request)


def _submit(audit, receipt_path, receipt_hash, digest, counter, start):
    class Shared(Client):
        def create_review(self, *args, **kwargs):
            with counter.get_lock():
                counter.value += 1
            time.sleep(0.05)
            return {"id": counter.value}

    request = ReviewRequest(
        "link",
        "org/repo",
        4,
        HEAD,
        EVIDENCE,
        receipt_path,
        receipt_hash,
        digest,
        "approve",
        request_id="shared",
    )
    start.wait()
    try:
        GitHubReviewBroker(
            Shared(),
            allowlisted_repositories={"org/repo"},
            reviewer="owner",
            audit_path=audit,
        ).submit_review(request)
    except BrokerError:
        pass


def test_cross_process_reservation_allows_one_external_action(tmp_path):
    path, file_hash, digest = receipt(tmp_path)
    counter, start = multiprocessing.Value("i", 0), multiprocessing.Event()
    processes = [
        multiprocessing.Process(
            target=_submit,
            args=(
                str(tmp_path / "audit.jsonl"),
                str(path),
                file_hash,
                digest,
                counter,
                start,
            ),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    start.set()
    for process in processes:
        process.join(5)
        assert process.exitcode == 0
    assert counter.value == 1


def test_malformed_input(tmp_path):
    with pytest.raises(BrokerError):
        broker(tmp_path).submit_review(req(tmp_path, pull_request=0))
