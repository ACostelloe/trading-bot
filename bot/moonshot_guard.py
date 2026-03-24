from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MoonshotDecision:
    allowed: bool
    score: int
    min_required: int
    reasons: list[str]


def _score_checks(checks_cfg: dict) -> int:
    checks = checks_cfg or {}
    return sum(1 for v in checks.values() if bool(v))


def _collect_red_flags(red_flags_cfg: dict) -> list[str]:
    red_flags = red_flags_cfg or {}
    triggered = [k for k, v in red_flags.items() if bool(v)]
    return triggered


def _symbol_in_list(symbol: str, values: list[str] | None) -> bool:
    if not values:
        return False
    s = symbol.upper()
    return any(str(v).upper() == s for v in values)


def evaluate_moonshot_entry(
    *,
    symbol: str,
    entry_notional: float,
    current_equity: float,
    open_positions_count: int,
    config: dict,
) -> MoonshotDecision:
    cfg = (config or {}).get("moonshot_checklist", {}) or {}
    if not cfg.get("enabled", False):
        return MoonshotDecision(True, 0, 0, ["moonshot_checklist_disabled"])

    checks_cfg = cfg.get("checks", {}) or {}
    red_flags_cfg = cfg.get("red_flags", {}) or {}

    score = _score_checks(checks_cfg)
    min_score = int(cfg.get("min_score_to_allow", 9))
    reasons: list[str] = []
    allowed = True

    # Score threshold gate.
    if score < min_score:
        allowed = False
        reasons.append(f"checklist_score_below_min({score}<{min_score})")

    # Red flags are hard blockers.
    triggered_flags = _collect_red_flags(red_flags_cfg)
    if triggered_flags:
        allowed = False
        reasons.append(f"red_flags_triggered={','.join(triggered_flags)}")

    # Position sizing hard caps.
    max_position_usdc = float(cfg.get("max_position_usdc", 15.0))
    if entry_notional > max_position_usdc:
        allowed = False
        reasons.append(f"position_cap_exceeded({entry_notional:.2f}>{max_position_usdc:.2f})")

    microcap_symbols = cfg.get("microcap_symbols", []) or []
    if _symbol_in_list(symbol, microcap_symbols):
        max_microcap_position_usdc = float(cfg.get("max_microcap_position_usdc", 7.0))
        if entry_notional > max_microcap_position_usdc:
            allowed = False
            reasons.append(
                f"microcap_position_cap_exceeded({entry_notional:.2f}>{max_microcap_position_usdc:.2f})"
            )

    presale_symbols = cfg.get("presale_symbols", []) or []
    if _symbol_in_list(symbol, presale_symbols):
        max_presale_position_usdc = float(cfg.get("max_presale_position_usdc", 5.0))
        if entry_notional > max_presale_position_usdc:
            allowed = False
            reasons.append(
                f"presale_position_cap_exceeded({entry_notional:.2f}>{max_presale_position_usdc:.2f})"
            )

    max_open_moonshots = int(cfg.get("max_open_moonshots", 8))
    if open_positions_count >= max_open_moonshots:
        allowed = False
        reasons.append(f"max_open_moonshots_reached({open_positions_count}>={max_open_moonshots})")

    max_total_drawdown_pause = float(cfg.get("max_total_drawdown_pause_pct", 25.0))
    start_balance = float(config.get("risk", {}).get("starting_balance_usdt", max(current_equity, 1.0)))
    if start_balance > 0:
        drawdown_pct = max(0.0, (start_balance - current_equity) / start_balance * 100.0)
        if drawdown_pct >= max_total_drawdown_pause:
            allowed = False
            reasons.append(
                f"drawdown_pause_triggered({drawdown_pct:.2f}%>={max_total_drawdown_pause:.2f}%)"
            )

    if not reasons:
        reasons.append("moonshot_gate_passed")
    return MoonshotDecision(allowed=allowed, score=score, min_required=min_score, reasons=reasons)
