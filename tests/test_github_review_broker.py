import json
import pytest
from skcoord.github_review_broker import BrokerError, GitHubReviewBroker, ReviewRequest

HEAD = "a" * 40
EVIDENCE = "b" * 64

class Client:
    def __init__(self, head=HEAD, checks=None, author="contributor"):
        self.head, self.checks, self.author, self.calls = head, checks or {"tests": "success"}, author, []
    def pull_request(self, repository, number): return {"head_sha": self.head, "author": self.author}
    def required_checks(self, repository, sha): return self.checks
    def create_review(self, repository, number, *, event, body):
        self.calls.append((repository, number, event, body)); return {"id": 7, "node_id": "n7", "html_url": "https://example/review/7", "token": "secret"}

def req(**kw):
    data = dict(caller="link", repository="org/repo", pull_request=4, head=HEAD, evidence_hash=EVIDENCE, decision="approve", request_id="r1")
    data.update(kw); return ReviewRequest(**data)

def broker(tmp_path, client=None):
    return GitHubReviewBroker(client or Client(), allowlisted_repositories={"org/repo"}, reviewer="owner", audit_path=tmp_path / "audit.jsonl")

def test_success_and_redaction(tmp_path):
    c = Client(); result = broker(tmp_path, c).submit_review(req())
    assert result["id"] == 7 and len(c.calls) == 1
    text = (tmp_path / "audit.jsonl").read_text(); assert "secret" not in text
    assert all(json.loads(line) for line in text.splitlines())

def test_rejections(tmp_path):
    for bad in (dict(repository="other/x"), dict(head="c" * 40), dict(evidence_hash="x" * 64), dict(decision="merge"), dict(body="please push")):
        with pytest.raises(BrokerError): broker(tmp_path).submit_review(req(**bad))
    with pytest.raises(BrokerError): broker(tmp_path, Client(checks={"tests": "failure"})).submit_review(req(request_id="failed"))
    with pytest.raises(BrokerError): broker(tmp_path, Client(head="c" * 40)).submit_review(req(request_id="drift"))

def test_replay(tmp_path):
    b = broker(tmp_path); b.submit_review(req())
    with pytest.raises(BrokerError, match="replayed"): b.submit_review(req())

def test_timeout_is_audited(tmp_path):
    class Timeout(Client):
        def pull_request(self, *args): raise TimeoutError
    with pytest.raises(BrokerError, match="timed out"): broker(tmp_path, Timeout()).submit_review(req())
    assert '"exit_status":124' in (tmp_path / "audit.jsonl").read_text()

def test_malformed_input(tmp_path):
    with pytest.raises(BrokerError): broker(tmp_path).submit_review(req(pull_request=0))
