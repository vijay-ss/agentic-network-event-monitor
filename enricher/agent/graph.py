from __future__ import annotations
import asyncio
from langgraph.graph import StateGraph, END
from shared.models import AgentState
from enricher.agent.tools.enrichment import run_all_enrichments
from enricher.agent.tools.scorer import score_event
from enricher.agent.tools.reasoner import reason
from enricher.agent.tools.router import route

def enrich_node(state: AgentState) -> dict:
    return asyncio.get_event_loop().run_until_complete(
        run_all_enrichments(state)
    )

def score_node(state: AgentState) -> dict:
    return score_event(state)

def reason_node(state: AgentState) -> dict:
    return reason(state)

def route_node(state: AgentState) -> dict:
    return route(state)

def should_reason(state: AgentState) -> str:
    """Skip LLM call for low signal events to save compute."""
    return "route" if state.threat_score < 15 else "reason"


def build_graph():
    graph = StateGraph(AgentState)

    graph.add_node("enrich", enrich_node)
    graph.add_node("score", score_node)
    graph.add_node("reason", reason_node)
    graph.add_node("route", route_node)

    graph.set_entry_point("enrich")
    graph.add_edge("enrich", "score")
    graph.add_conditional_edges(
        "score",
        should_reason,
        {"reason": "reason", "route": "route"}
    )
    graph.add_edge("reason", "route")
    graph.add_edge("route", END)

    return graph.compile()