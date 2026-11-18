"""项目测值质检：十进制上下限规则、逐行核对结论与原子重算。

结论状态机（对每份报告的每一行测值逐一核对）：

* ``analyzable``（可分析）：所有数据行的项目都已配置规则，且每个测值都是
  十进制数值并落在 ``[下限, 上限]`` 闭区间内；
* ``pending_review``（待复核）：至少一行越界或非数值——结论中记录行号与
  原值，不能直接交给分析；
* ``undetermined``（未判定）：其余情况，即没有越界行但存在未配置规则的
  项目行，无法给出结论。

一致性规则：

* 新到报告在入仓事务内即时判定（见 ``collector``），结论与报告同生同灭；
* 规则修订在**同一个事务**内完成：版本号 +1、``rules_generation`` +1、
  所有用到旧版规则（或当时没有该规则）的结论标 ``stale``（过期）；
* 重算把过期/缺失的结论整体替换：逐行计算在事务外进行，发布在**单个事务**
  内完成；发布事务会重新检查 ``rules_generation``（规则在重算期间再次变化
  则放弃本次发布并重试），并补算快照之后入仓的报告，因此新结论始终覆盖
  同期入仓报告。任何失败都不会留下半份结论，可直接重试。
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable

from .db import Database, transaction, utcnow

log = logging.getLogger("bridge.qc")

STATUS_ANALYZABLE = "analyzable"
STATUS_PENDING_REVIEW = "pending_review"
STATUS_UNDETERMINED = "undetermined"

STATUS_LABELS = {
    STATUS_ANALYZABLE: "可分析",
    STATUS_PENDING_REVIEW: "待复核",
    STATUS_UNDETERMINED: "未判定",
}

# 十进制数值：可选符号、整数/小数、可选科学计数法指数。NaN/Inf 不接受。
_DECIMAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
MAX_DECIMAL_LEN = 64
MAX_ITEM_LEN = 100


class RuleInvalid(ValueError):
    """The submitted rule (item / limits) is not acceptable."""


class QcBusy(RuntimeError):
    """Another instance currently owns the recompute lease."""


class QcRecomputeFailed(RuntimeError):
    """Recompute gave up; nothing was published, retry is safe."""


class _RulesChanged(RuntimeError):
    """Rules moved while a recompute was computing: abort and retry."""


class _CatchUpTooBig(RuntimeError):
    """Too many reports arrived during one publish: precompute them next round."""


def parse_decimal(text: str) -> Decimal | None:
    """Parse a decimal string, returning ``None`` for non-numeric input."""
    value = text.strip()
    if not value or len(value) > MAX_DECIMAL_LEN or not _DECIMAL_RE.match(value):
        return None
    try:
        return Decimal(value)
    except InvalidOperation:  # pragma: no cover - regex already filtered
        return None


@dataclass(frozen=True)
class Rule:
    item: str
    lower: Decimal
    upper: Decimal
    version: int
    lower_text: str
    upper_text: str


def compute_verdict(rows: list[list[str]], rules: dict[str, Rule]) -> dict[str, Any]:
    """Check every data row of one report against the configured rules.

    ``rows`` are the warehoused CSV data rows ``[报告号, 样本号, 项目, 测值]``;
    the reported line number follows the collector's convention (header is
    line 1, first data row is line 2).
    """
    findings: list[dict[str, Any]] = []
    uncovered: list[dict[str, Any]] = []
    used: dict[str, int] = {}
    for lineno, row in enumerate(rows, start=2):
        item = row[2]
        value = row[3]
        rule = rules.get(item)
        if rule is None:
            uncovered.append({"lineno": lineno, "item": item, "value": value})
            continue
        used[item] = rule.version
        number = parse_decimal(value)
        if number is None:
            findings.append(
                {
                    "lineno": lineno,
                    "item": item,
                    "value": value,
                    "kind": "non_numeric",
                    "lower": rule.lower_text,
                    "upper": rule.upper_text,
                }
            )
        elif number < rule.lower or number > rule.upper:
            findings.append(
                {
                    "lineno": lineno,
                    "item": item,
                    "value": value,
                    "kind": "out_of_range",
                    "lower": rule.lower_text,
                    "upper": rule.upper_text,
                }
            )
    if findings:
        status = STATUS_PENDING_REVIEW
    elif uncovered:
        status = STATUS_UNDETERMINED
    else:
        status = STATUS_ANALYZABLE
    return {
        "status": status,
        "findings": findings,
        "uncovered": uncovered,
        "rule_versions": used,
    }


# ---------------------------------------------------------------------------
# transaction-local helpers (shared by the collector and the recompute engine)
# ---------------------------------------------------------------------------
def rules_map_tx(conn) -> dict[str, Rule]:
    rows = conn.execute("SELECT * FROM qc_rule").fetchall()
    return {
        row["item"]: Rule(
            item=row["item"],
            lower=Decimal(row["lower_limit"]),
            upper=Decimal(row["upper_limit"]),
            version=int(row["version"]),
            lower_text=row["lower_limit"],
            upper_text=row["upper_limit"],
        )
        for row in rows
    }


def write_report_items_tx(conn, report_id: int, rows: Iterable[list[str]]) -> None:
    items = sorted({row[2] for row in rows})
    conn.executemany(
        "INSERT OR IGNORE INTO report_item (report_id, item) VALUES (?, ?)",
        [(report_id, item) for item in items],
    )


def write_verdict_tx(conn, report_id: int, verdict: dict[str, Any], instance: str) -> None:
    """Upsert one verdict and its rule-version footprint; clears staleness."""
    conn.execute(
        """INSERT INTO qc_verdict
           (report_id, status, findings_json, uncovered_json, stale,
            stale_reason, computed_at, instance)
           VALUES (?, ?, ?, ?, 0, NULL, ?, ?)
           ON CONFLICT(report_id) DO UPDATE SET
               status=excluded.status,
               findings_json=excluded.findings_json,
               uncovered_json=excluded.uncovered_json,
               stale=0, stale_reason=NULL,
               computed_at=excluded.computed_at,
               instance=excluded.instance""",
        (
            report_id,
            verdict["status"],
            json.dumps(verdict["findings"], ensure_ascii=False),
            json.dumps(verdict["uncovered"], ensure_ascii=False),
            utcnow(),
            instance,
        ),
    )
    conn.execute("DELETE FROM qc_verdict_rule WHERE report_id=?", (report_id,))
    conn.executemany(
        "INSERT INTO qc_verdict_rule (report_id, item, rule_version) VALUES (?, ?, ?)",
        [(report_id, item, version) for item, version in verdict["rule_versions"].items()],
    )


def _rules_generation_tx(conn) -> int:
    row = conn.execute(
        "SELECT value FROM qc_meta WHERE key='rules_generation'"
    ).fetchone()
    return int(row["value"]) if row else 0


class QcEngine:
    """Rule writes and the atomic recompute, coordinated across instances."""

    def __init__(
        self,
        db: Database,
        instance_id: str,
        *,
        lease_seconds: float = 30.0,
        catch_up_limit: int = 500,
        max_attempts: int = 5,
    ):
        self.db = db
        self.instance_id = instance_id
        self.lease_seconds = lease_seconds
        self.catch_up_limit = catch_up_limit
        self.max_attempts = max_attempts

    # -------------------------------------------------------------- rules
    def save_rule(self, item: str, lower: str, upper: str) -> dict[str, Any]:
        """Create or revise the decimal limits for one item.

        The rule write, the generation bump and the stale-marking of outdated
        verdicts happen in one transaction, so a conclusion is never current
        under a rule it was not computed with.  Submitting the existing limits
        is a no-op (no version bump, nothing marked stale).
        """
        item = (item or "").strip()
        lower_text = (lower or "").strip()
        upper_text = (upper or "").strip()
        if not item:
            raise RuleInvalid("项目不能为空")
        if len(item) > MAX_ITEM_LEN:
            raise RuleInvalid(f"项目名超过 {MAX_ITEM_LEN} 字符")
        lower_dec = parse_decimal(lower_text)
        if lower_dec is None:
            raise RuleInvalid(f"下限不是十进制数值：{lower_text!r}")
        upper_dec = parse_decimal(upper_text)
        if upper_dec is None:
            raise RuleInvalid(f"上限不是十进制数值：{upper_text!r}")
        if lower_dec > upper_dec:
            raise RuleInvalid(f"下限 {lower_text} 大于上限 {upper_text}")

        with self.db.connect() as conn:
            with transaction(conn):
                row = conn.execute(
                    "SELECT * FROM qc_rule WHERE item=?", (item,)
                ).fetchone()
                if row is not None and (
                    Decimal(row["lower_limit"]) == lower_dec
                    and Decimal(row["upper_limit"]) == upper_dec
                ):
                    return {
                        "change": "unchanged",
                        "item": item,
                        "version": int(row["version"]),
                        "staled": 0,
                        "generation": _rules_generation_tx(conn),
                    }
                version = 1 if row is None else int(row["version"]) + 1
                conn.execute(
                    """INSERT INTO qc_rule
                       (item, lower_limit, upper_limit, version, updated_at, updated_by)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(item) DO UPDATE SET
                           lower_limit=excluded.lower_limit,
                           upper_limit=excluded.upper_limit,
                           version=excluded.version,
                           updated_at=excluded.updated_at,
                           updated_by=excluded.updated_by""",
                    (item, lower_text, upper_text, version, utcnow(), self.instance_id),
                )
                generation = self._bump_generation_tx(conn)
                # Verdicts for reports containing this item are outdated when
                # they used an older rule version, or no rule at all for it.
                reason = f"规则 {item} 已修订为 v{version}"
                cur = conn.execute(
                    """UPDATE qc_verdict SET stale=1, stale_reason=?
                       WHERE report_id IN (
                           SELECT ri.report_id FROM report_item ri
                           LEFT JOIN qc_verdict_rule vr
                             ON vr.report_id = ri.report_id AND vr.item = ?
                           WHERE ri.item = ?
                             AND (vr.rule_version IS NULL OR vr.rule_version < ?)
                       )""",
                    (reason, item, item, version),
                )
                log.info(
                    "qc rule %s -> v%s by %s: %d verdict(s) marked stale",
                    item, version, self.instance_id, cur.rowcount,
                )
                return {
                    "change": "created" if row is None else "revised",
                    "item": item,
                    "version": version,
                    "staled": int(cur.rowcount),
                    "generation": generation,
                }

    def _bump_generation_tx(self, conn) -> int:
        generation = _rules_generation_tx(conn) + 1
        conn.execute(
            """INSERT INTO qc_meta (key, value) VALUES ('rules_generation', ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (str(generation),),
        )
        return generation

    # ----------------------------------------------------------- recompute
    def recompute(
        self,
        before_publish: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        """Replace every stale/missing verdict atomically.

        Rows are checked outside the write transaction so the other instance
        can keep ingesting mail; the publish itself is a single transaction
        that (a) verifies the rules did not move and (b) also computes reports
        that arrived after the snapshot, so the new conclusions always cover
        same-period arrivals.  ``before_publish`` is a test hook executed
        between snapshot and publish.
        """
        token = self._claim_lease()
        if token is None:
            raise QcBusy("另一实例正在重算质检结论，请稍后重试")
        try:
            for attempt in range(1, self.max_attempts + 1):
                self._refresh_lease(token)
                with self.db.connect() as conn:
                    generation = _rules_generation_tx(conn)
                    rules = rules_map_tx(conn)
                pending = self.db.qc_pending_reports()
                verdicts = []
                for row in pending:
                    rows = json.loads(row["rows_json"])
                    verdicts.append(
                        {
                            "report_id": int(row["id"]),
                            "rows": rows,
                            "verdict": compute_verdict(rows, rules),
                        }
                    )
                if before_publish is not None:
                    before_publish()
                try:
                    result = self._publish(verdicts, generation)
                except (_RulesChanged, _CatchUpTooBig) as exc:
                    log.info("recompute attempt %d aborted, retrying: %s", attempt, exc)
                    continue
                result["attempts"] = attempt
                log.info("recompute done: %s", result)
                return result
            raise QcRecomputeFailed(
                f"重算连续 {self.max_attempts} 次被并发变更打断，未发布任何结论；请重试"
            )
        finally:
            self._release_lease(token)

    def try_recompute(self) -> dict[str, Any]:
        """Recompute without raising for busy/failed outcomes (web actions)."""
        try:
            return self.recompute()
        except QcBusy as exc:
            return {"status": "busy", "error": str(exc)}
        except QcRecomputeFailed as exc:
            return {"status": "failed", "error": str(exc)}

    def _publish(self, verdicts: list[dict[str, Any]], generation: int) -> dict[str, Any]:
        """Write all verdicts in one transaction — all or nothing.

        The transaction re-reads ``rules_generation`` and re-queries for
        reports still missing a (current) verdict, so reports warehoused
        between the snapshot and this commit are covered as well.
        """
        with self.db.connect() as conn:
            with transaction(conn):
                current = _rules_generation_tx(conn)
                if current != generation:
                    raise _RulesChanged(
                        f"规则代数由 {generation} 变为 {current}，本次发布作废"
                    )
                known = {v["report_id"] for v in verdicts}
                extra_rows = conn.execute(
                    """SELECT r.id, r.rows_json FROM report r
                       LEFT JOIN qc_verdict v ON v.report_id = r.id
                       WHERE v.report_id IS NULL OR v.stale = 1
                       ORDER BY r.id"""
                ).fetchall()
                extra = [row for row in extra_rows if int(row["id"]) not in known]
                if len(extra) > self.catch_up_limit:
                    raise _CatchUpTooBig(
                        f"发布期间新增 {len(extra)} 份待算报告，超过单次补算上限 "
                        f"{self.catch_up_limit}，下一轮将预先计算"
                    )
                rules = rules_map_tx(conn)
                for item in verdicts:
                    write_report_items_tx(conn, item["report_id"], item["rows"])
                    write_verdict_tx(
                        conn, item["report_id"], item["verdict"], self.instance_id
                    )
                for row in extra:
                    rows = json.loads(row["rows_json"])
                    write_report_items_tx(conn, int(row["id"]), rows)
                    write_verdict_tx(
                        conn,
                        int(row["id"]),
                        compute_verdict(rows, rules),
                        self.instance_id,
                    )
                return {
                    "status": "completed",
                    "written": len(verdicts) + len(extra),
                    "caught_up": len(extra),
                }

    # --------------------------------------------------------------- lease
    def _claim_lease(self) -> str | None:
        token = uuid.uuid4().hex
        now = time.time()
        with self.db.connect() as conn:
            with transaction(conn):
                row = conn.execute(
                    "SELECT value FROM qc_meta WHERE key='recompute_lease'"
                ).fetchone()
                if row is not None:
                    try:
                        lease = json.loads(row["value"])
                    except json.JSONDecodeError:
                        lease = {}
                    if lease.get("token") and float(lease.get("expires", 0)) > now:
                        return None
                payload = {
                    "token": token,
                    "instance": self.instance_id,
                    "expires": now + self.lease_seconds,
                }
                conn.execute(
                    """INSERT INTO qc_meta (key, value) VALUES ('recompute_lease', ?)
                       ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                    (json.dumps(payload),),
                )
                return token

    def _refresh_lease(self, token: str) -> None:
        now = time.time()
        with self.db.connect() as conn:
            with transaction(conn):
                row = conn.execute(
                    "SELECT value FROM qc_meta WHERE key='recompute_lease'"
                ).fetchone()
                if row is None:
                    return
                try:
                    lease = json.loads(row["value"])
                except json.JSONDecodeError:
                    return
                if lease.get("token") != token:
                    return
                lease["expires"] = now + self.lease_seconds
                conn.execute(
                    "UPDATE qc_meta SET value=? WHERE key='recompute_lease'",
                    (json.dumps(lease),),
                )

    def _release_lease(self, token: str) -> None:
        with self.db.connect() as conn:
            with transaction(conn):
                row = conn.execute(
                    "SELECT value FROM qc_meta WHERE key='recompute_lease'"
                ).fetchone()
                if row is None:
                    return
                try:
                    lease = json.loads(row["value"])
                except json.JSONDecodeError:
                    lease = {}
                if lease.get("token") == token:
                    conn.execute("DELETE FROM qc_meta WHERE key='recompute_lease'")


__all__ = [
    "QcBusy",
    "QcEngine",
    "QcRecomputeFailed",
    "Rule",
    "RuleInvalid",
    "STATUS_ANALYZABLE",
    "STATUS_LABELS",
    "STATUS_PENDING_REVIEW",
    "STATUS_UNDETERMINED",
    "compute_verdict",
    "parse_decimal",
    "rules_map_tx",
    "write_report_items_tx",
    "write_verdict_tx",
]
