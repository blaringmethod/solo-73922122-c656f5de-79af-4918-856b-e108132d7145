"""QC rules, per-row verdicts, stale marking and atomic recompute."""

from __future__ import annotations

import json
import threading

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.qc import (
    QcBusy,
    QcEngine,
    RuleInvalid,
    compute_verdict,
    parse_decimal,
)


def _engine(demo, instance: str = "inst-a"):
    collector = demo.make_collector(instance)
    engine = QcEngine(
        collector.db, instance, lease_seconds=30, catch_up_limit=500, max_attempts=5
    )
    return collector, engine


def _verdicts_by_report_no(collector):
    out = {}
    for report in collector.db.list_reports(500):
        verdict = collector.db.qc_verdict_for_report(report["id"])
        out[report["report_no"]] = (report, verdict)
    return out


def _delete_qc_rows(collector, report_no: str) -> None:
    """Simulate an old-instance ingest that wrote no QC rows at all."""
    with collector.db.connect() as conn:
        row = conn.execute(
            "SELECT id FROM report WHERE report_no=?", (report_no,)
        ).fetchone()
        assert row is not None
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM qc_verdict WHERE report_id=?", (row["id"],))
        conn.execute("DELETE FROM qc_verdict_rule WHERE report_id=?", (row["id"],))
        conn.execute("DELETE FROM report_item WHERE report_id=?", (row["id"],))
        conn.execute("COMMIT")


# --------------------------------------------------------------------- rules
def test_decimal_parser():
    assert parse_decimal("12") is not None
    assert parse_decimal("-3.5") is not None
    assert parse_decimal(".5") is not None
    assert parse_decimal("1e3") is not None
    assert parse_decimal("abc") is None
    assert parse_decimal("") is None
    assert parse_decimal("NaN") is None
    assert parse_decimal("inf") is None
    assert parse_decimal("1,5") is None
    assert parse_decimal("1_000") is None


def test_rule_validation(demo):
    _, engine = _engine(demo)
    with pytest.raises(RuleInvalid):
        engine.save_rule("", "0", "1")
    with pytest.raises(RuleInvalid):
        engine.save_rule("ALT", "abc", "1")
    with pytest.raises(RuleInvalid):
        engine.save_rule("ALT", "0", "xyz")
    with pytest.raises(RuleInvalid):
        engine.save_rule("ALT", "10", "1")  # lower > upper
    change = engine.save_rule("ALT", "0", "40")
    assert change["change"] == "created" and change["version"] == 1


def test_same_limits_save_is_noop(demo):
    _, engine = _engine(demo)
    engine.save_rule("ALT", "0", "40")
    again = engine.save_rule("ALT", "0.0", "40.00")  # numerically identical
    assert again["change"] == "unchanged"
    assert again["version"] == 1
    assert again["staled"] == 0
    rules = engine.db.qc_rules()
    assert len(rules) == 1 and rules[0]["version"] == 1


# ------------------------------------------------------------------ verdicts
def test_ingest_computes_verdicts_with_line_and_value(demo):
    collector, engine = _engine(demo)
    engine.save_rule("ALT", "0", "40")
    demo.deliver(
        "qc mail 1",
        [
            demo.attachment("Q1", value="21"),   # in range
            demo.attachment("Q2", value="999"),  # out of range
            demo.csv_attachment(
                "Q3.csv", "报告号,样本号,项目,测值\nQ3,S3,ALT,abc\nQ3,S3,ALT,30\n"
            ),                                    # non-numeric on line 2
        ],
    )
    demo.deliver(
        "qc mail 2",
        [
            demo.attachment(
                "Q4",
                rows=[{"report_no": "Q4", "sample_no": "S4", "item": "GLU", "value": "5.4"}],
            ),                                    # no rule for GLU
        ],
    )
    collector.scan()
    verdicts = _verdicts_by_report_no(collector)

    _, q1 = verdicts["Q1"]
    assert q1["status"] == "analyzable"
    assert json.loads(q1["findings_json"]) == []

    _, q2 = verdicts["Q2"]
    assert q2["status"] == "pending_review"
    findings = json.loads(q2["findings_json"])
    assert findings[0]["kind"] == "out_of_range"
    assert findings[0]["lineno"] == 2
    assert findings[0]["value"] == "999"
    assert findings[0]["lower"] == "0" and findings[0]["upper"] == "40"

    _, q3 = verdicts["Q3"]
    assert q3["status"] == "pending_review"
    findings = json.loads(q3["findings_json"])
    assert len(findings) == 1  # the second row (30) is in range
    assert findings[0]["kind"] == "non_numeric"
    assert findings[0]["lineno"] == 2
    assert findings[0]["value"] == "abc"

    _, q4 = verdicts["Q4"]
    assert q4["status"] == "undetermined"
    uncovered = json.loads(q4["uncovered_json"])
    assert uncovered[0]["item"] == "GLU" and uncovered[0]["lineno"] == 2


def test_no_rules_at_all_means_undetermined(demo):
    collector, _ = _engine(demo)
    demo.deliver("no rules", [demo.attachment("Q1", value="12")])
    collector.scan()
    _, verdict = _verdicts_by_report_no(collector)["Q1"]
    assert verdict["status"] == "undetermined"


def test_boundary_values_are_inclusive(demo):
    _, engine = _engine(demo)
    engine.save_rule("ALT", "0", "40")
    rules = {r["item"]: r for r in engine.db.qc_rules()}
    from app.qc import rules_map_tx  # local import to keep the test focused

    with engine.db.connect() as conn:
        rule_map = rules_map_tx(conn)
    verdict = compute_verdict(
        [["Q", "S", "ALT", "0"], ["Q", "S", "ALT", "40"]], rule_map
    )
    assert verdict["status"] == "analyzable"
    verdict = compute_verdict([["Q", "S", "ALT", "40.0001"]], rule_map)
    assert verdict["status"] == "pending_review"
    del rules


# ------------------------------------------------------- revision and stale
def test_revision_marks_only_affected_verdicts_stale(demo):
    collector, engine = _engine(demo)
    engine.save_rule("ALT", "0", "40")
    engine.save_rule("GLU", "3", "6")
    demo.deliver(
        "three reports",
        [
            demo.attachment("Q1", value="21"),
            demo.attachment("Q2", value="999"),
            demo.attachment(
                "Q3",
                rows=[{"report_no": "Q3", "sample_no": "S3", "item": "GLU", "value": "5"}],
            ),
        ],
    )
    collector.scan()
    before = _verdicts_by_report_no(collector)
    assert before["Q1"][1]["status"] == "analyzable"

    change = engine.save_rule("ALT", "0", "15")
    assert change["change"] == "revised" and change["version"] == 2
    assert change["staled"] == 2  # Q1 + Q2, not the GLU report

    after = _verdicts_by_report_no(collector)
    assert after["Q1"][1]["stale"] == 1
    assert after["Q2"][1]["stale"] == 1
    assert after["Q3"][1]["stale"] == 0  # unrelated rule untouched

    result = engine.recompute()
    assert result["status"] == "completed"
    assert result["written"] == 2

    final = _verdicts_by_report_no(collector)
    assert final["Q1"][1]["status"] == "pending_review"  # 21 > 15 now
    assert final["Q1"][1]["stale"] == 0
    assert final["Q2"][1]["status"] == "pending_review"
    assert final["Q3"][1]["status"] == "analyzable"
    # The new verdict records the new rule version.
    with collector.db.connect() as conn:
        rows = conn.execute(
            """SELECT item, rule_version FROM qc_verdict_rule
               WHERE report_id=?""",
            (final["Q1"][0]["id"],),
        ).fetchall()
    assert {(r["item"], r["rule_version"]) for r in rows} == {("ALT", 2)}


def test_new_rule_for_previously_uncovered_item_stales_verdict(demo):
    collector, engine = _engine(demo)
    demo.deliver(
        "uncovered",
        [
            demo.attachment(
                "Q1",
                rows=[{"report_no": "Q1", "sample_no": "S1", "item": "GLU", "value": "9"}],
            )
        ],
    )
    collector.scan()
    assert _verdicts_by_report_no(collector)["Q1"][1]["status"] == "undetermined"

    change = engine.save_rule("GLU", "3", "6")
    assert change["staled"] == 1  # verdict existed without any rule for GLU
    engine.recompute()
    verdict = _verdicts_by_report_no(collector)["Q1"][1]
    assert verdict["status"] == "pending_review"  # 9 > 6
    assert verdict["stale"] == 0


def test_legacy_reports_without_verdicts_are_computed(demo):
    collector, engine = _engine(demo)
    engine.save_rule("ALT", "0", "40")
    demo.deliver("legacy", [demo.attachment("Q1", value="21")])
    collector.scan()
    _delete_qc_rows(collector, "Q1")  # as if ingested before QC existed
    assert collector.db.qc_verdict_for_report(
        _verdicts_by_report_no(collector)["Q1"][0]["id"]
    ) is None

    result = engine.recompute()
    assert result["written"] == 1
    _, verdict = _verdicts_by_report_no(collector)["Q1"]
    assert verdict["status"] == "analyzable"


# ---------------------------------------------------- recompute concurrency
def test_recompute_covers_reports_arriving_during_recompute(demo):
    collector_a, engine_a = _engine(demo, "inst-a")
    collector_b = demo.make_collector("inst-b")
    engine_a.save_rule("ALT", "0", "40")
    demo.deliver("first", [demo.attachment("Q1", value="21")])
    collector_a.scan()

    def concurrent_ingest():
        # The other instance keeps receiving mail while we recompute; its
        # ingest may even lack QC rows (rolling upgrade).  The publish must
        # still cover the report.
        demo.deliver("late arrival", [demo.attachment("Q9", value="10")])
        result = collector_b.scan()
        assert result.counts["accepted"] == 1
        _delete_qc_rows(collector_b, "Q9")

    result = engine_a.recompute(before_publish=concurrent_ingest)
    assert result["caught_up"] == 1
    verdicts = _verdicts_by_report_no(collector_a)
    assert verdicts["Q9"][1] is not None
    assert verdicts["Q9"][1]["status"] == "analyzable"
    assert verdicts["Q9"][1]["stale"] == 0


def test_scan_proceeds_while_recompute_runs(demo):
    collector_a, engine_a = _engine(demo, "inst-a")
    collector_b = demo.make_collector("inst-b")
    engine_a.save_rule("ALT", "0", "40")
    for index in range(5):
        demo.deliver(f"m{index}", [demo.attachment(f"Q{index}", value="50")])
    collector_a.scan()
    engine_a.save_rule("ALT", "0", "45")  # everything becomes stale

    gate = threading.Event()
    outcome = {}

    def slow_hook():
        gate.wait(timeout=10)

    def run_recompute():
        outcome["recompute"] = engine_a.recompute(before_publish=slow_hook)

    worker = threading.Thread(target=run_recompute)
    worker.start()
    try:
        demo.deliver("during recompute", [demo.attachment("QZ", value="30")])
        outcome["scan"] = collector_b.scan()  # must not be blocked
    finally:
        gate.set()
        worker.join(timeout=30)
    assert not worker.is_alive()
    assert outcome["scan"].counts["accepted"] == 1
    assert outcome["recompute"]["status"] == "completed"
    for report_no, (_, verdict) in _verdicts_by_report_no(collector_a).items():
        assert verdict is not None and verdict["stale"] == 0, report_no


def test_concurrent_recompute_is_refused(demo):
    _, engine_a = _engine(demo, "inst-a")
    _, engine_b = _engine(demo, "inst-b")
    token = engine_a._claim_lease()
    assert token is not None
    try:
        with pytest.raises(QcBusy):
            engine_b.recompute()
    finally:
        engine_a._release_lease(token)
    # After release the other instance can run.
    assert engine_b.recompute()["status"] == "completed"


def test_rule_change_during_recompute_aborts_and_retries(demo):
    collector, engine = _engine(demo, "inst-a")
    _, engine_b = _engine(demo, "inst-b")
    engine.save_rule("ALT", "0", "40")
    demo.deliver("one", [demo.attachment("Q1", value="21")])
    collector.scan()

    calls = {"n": 0}

    def revise_rules():
        calls["n"] += 1
        if calls["n"] == 1:
            engine_b.save_rule("ALT", "0", "10")  # concurrent revision

    result = engine.recompute(before_publish=revise_rules)
    assert result["attempts"] >= 2
    _, verdict = _verdicts_by_report_no(collector)["Q1"]
    assert verdict["status"] == "pending_review"  # computed with v2 (0..10)
    assert verdict["stale"] == 0


def test_failed_publish_is_atomic_and_retryable(demo, monkeypatch):
    collector, engine = _engine(demo)
    engine.save_rule("ALT", "0", "40")
    demo.deliver("two", [demo.attachment("Q1", value="21"), demo.attachment("Q2", value="22")])
    collector.scan()
    engine.save_rule("ALT", "0", "15")  # two verdicts become stale

    import app.qc as qc

    real_write = qc.write_verdict_tx
    calls = {"n": 0}

    def boom(conn, report_id, verdict, instance):
        calls["n"] += 1
        real_write(conn, report_id, verdict, instance)
        raise RuntimeError("simulated publish crash")

    monkeypatch.setattr(qc, "write_verdict_tx", boom)
    with pytest.raises(RuntimeError, match="simulated publish crash"):
        engine.recompute()
    monkeypatch.setattr(qc, "write_verdict_tx", real_write)

    # Nothing half-published: both verdicts are still the old, stale ones.
    verdicts = _verdicts_by_report_no(collector)
    assert verdicts["Q1"][1]["stale"] == 1
    assert verdicts["Q2"][1]["stale"] == 1
    assert verdicts["Q1"][1]["status"] == "analyzable"  # old conclusion kept

    # Retry succeeds and converges.
    result = engine.recompute()
    assert result["status"] == "completed"
    verdicts = _verdicts_by_report_no(collector)
    assert verdicts["Q1"][1]["status"] == "pending_review"
    assert verdicts["Q1"][1]["stale"] == 0
    assert verdicts["Q2"][1]["status"] == "pending_review"


# ----------------------------------------------------------------------- web
def _client(demo, instance: str = "inst-a") -> TestClient:
    collector = demo.make_collector(instance)
    app = create_app(collector.cfg)
    app.state.collector = collector
    app.state.db = collector.db
    app.state.store = collector.store
    return TestClient(app)


def test_qc_page_api_and_actions(demo):
    client = _client(demo)
    # Create a rule through the JSON API (with automatic recompute).
    response = client.post(
        "/api/qc/rules", json={"item": "ALT", "lower": "0", "upper": "40"}
    )
    assert response.status_code == 200
    assert response.json()["rule"]["change"] == "created"
    assert response.json()["recompute"]["status"] == "completed"

    demo.deliver("web qc", [demo.attachment("Q1", value="999")])
    client.post("/api/scan")

    verdicts = client.get("/api/qc/verdicts").json()
    assert len(verdicts) == 1
    assert verdicts[0]["status"] == "pending_review"
    assert verdicts[0]["findings"][0]["lineno"] == 2
    assert verdicts[0]["findings"][0]["value"] == "999"

    # The QC page shows the finding with line number and original value.
    page = client.get("/qc").text
    assert "待复核" in page
    assert "第 2 行" in page and "999" in page
    assert "ALT" in page

    # Report-level endpoint.
    report_id = client.get("/api/messages").json()[0]["reports"][0]["report_id"]
    detail = client.get(f"/api/reports/{report_id}/qc").json()
    assert detail["verdict"]["status"] == "pending_review"

    # Revise the rule via the HTML form action; old conclusion is recomputed.
    response = client.post(
        "/actions/qc/rule",
        data={"item": "ALT", "lower": "0", "upper": "1000"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    page = client.get("/qc").text
    assert "v2" in page
    verdicts = client.get("/api/qc/verdicts").json()
    assert verdicts[0]["status"] == "analyzable"  # 999 <= 1000 now
    assert verdicts[0]["stale"] is False

    # Manual recompute action is always available (retry path).
    assert client.post("/actions/qc/recompute", follow_redirects=False).status_code == 303

    # Dashboard surfaces QC counts.
    dashboard = client.get("/").text
    assert "待复核" in dashboard and "可分析" in dashboard


def test_qc_rule_api_validation_and_stale_filter(demo):
    client = _client(demo)
    bad = client.post("/api/qc/rules", json={"item": "ALT", "lower": "x", "upper": "1"})
    assert bad.status_code == 422

    client.post("/api/qc/rules", json={"item": "ALT", "lower": "0", "upper": "40"})
    demo.deliver("stale demo", [demo.attachment("Q1", value="21")])
    client.post("/api/scan")

    # Revise without recomputing so the stale state is observable.
    response = client.post(
        "/api/qc/rules",
        json={"item": "ALT", "lower": "0", "upper": "15", "recompute": False},
    )
    assert response.json()["rule"]["staled"] == 1
    stale = client.get("/api/qc/verdicts", params={"stale": 1}).json()
    assert len(stale) == 1 and stale[0]["stale"] is True
    assert "过期" in client.get("/qc", params={"stale": "1"}).text

    # Retry publishes the new conclusions and clears staleness.
    result = client.post("/api/qc/recompute")
    assert result.status_code == 200
    assert result.json()["written"] == 1
    assert client.get("/api/qc/verdicts", params={"stale": 1}).json() == []
    verdict = client.get("/api/qc/verdicts").json()[0]
    assert verdict["status"] == "pending_review"  # 21 > 15


def test_qc_recompute_conflict_returns_409(demo):
    client = _client(demo)
    token = client.app.state.qc._claim_lease()
    assert token is not None
    try:
        response = client.post("/api/qc/recompute")
        assert response.status_code == 409
    finally:
        client.app.state.qc._release_lease(token)
    assert client.post("/api/qc/recompute").status_code == 200
