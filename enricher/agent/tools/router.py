from __future__ import annotations
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich import box
from shared.models import AgentState, ActionTier

console = Console()

ACTION_COLORS = {
    ActionTier.ARCHIVE:  "dim",
    ActionTier.DIGEST:   "blue",
    ActionTier.TICKET:   "yellow",
    ActionTier.PAGE:     "red",
    ActionTier.CONTAIN:  "bold white on red",
}


def route(state: AgentState) -> dict:
    action = state.recommended_action or ActionTier.ARCHIVE

    # ── Dispatch handlers (swap for real integrations) ────────────────────
    if action == ActionTier.TICKET:
        # SWAP: jira.create_issue(...)
        pass
    elif action == ActionTier.PAGE:
        # SWAP: pagerduty.trigger(...)
        pass
    elif action == ActionTier.CONTAIN:
        # SWAP: firewall.block_ip(state.silver.source_ip)
        #       idp.suspend_user(state.silver.user)
        pass

    _print_summary(state, action)
    return {"routed": True}


def _print_summary(state: AgentState, action: ActionTier):
    s = state.silver
    color = ACTION_COLORS.get(action, "white")

    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    table.add_column("Dimension", style="dim")
    table.add_column("Score", justify="right")
    for k, v in state.score_breakdown.items():
        if k != "composite":
            table.add_row(k.replace("_", " ").title(), str(v))
    table.add_row("[bold]COMPOSITE[/bold]", f"[bold]{state.threat_score}/100[/bold]")

    mitre = f"\n  MITRE: {state.mitre_tactic}" if state.mitre_tactic else ""

    panel = Panel(
        Group(
            f"[bold]{s.event_type}[/bold] | {s.source_ip or 'N/A'} → "
            f"{s.destination_domain or s.destination_ip or 'N/A'} | "
            f"user: {s.user or 'N/A'} | zone: {s.security_zone or 'N/A'}\n",
            table,
            f"\n[italic]{state.narrative}[/italic]{mitre}",
        ),
        title=f"[{color}] {action.upper()} [/{color}]  id={s.id[:8]}",
        border_style="red" if "on" in color else color,
    )
    console.print(panel)