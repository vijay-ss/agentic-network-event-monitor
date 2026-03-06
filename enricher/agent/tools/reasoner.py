from __future__ import annotations
import re
import json
from langchain_ollama import ChatOllama
from langchain_core.messages import SystemMessage, HumanMessage
from shared.models import AgentState, ActionTier
from shared.config import config

llm = ChatOllama(
    base_url=config.ollama.base_url,
    model=config.ollama.model,
    temperature=config.ollama.temperature,
    num_predict=config.ollama.max_tokens,
)

SYSTEM_PROMPT = """You are an expert security analyst. You receive enriched network security log data and produce a structured assessment.

Respond with valid JSON only — no markdown, no explanation outside the JSON.

Response schema:
{
  "recommended_action": "<archive | digest | ticket | page | contain>",
  "mitre_tactic": "<MITRE ATT&CK tactic name or null>",
  "narrative": "<2-3 sentence plain-English analyst summary explaining the threat, context, and recommended action>"
}

Action definitions:
- archive:  Noise / informational. No action needed.
- digest:   Low signal. Include in daily summary.
- ticket:   Medium risk. Create analyst queue ticket.
- page:     High risk. Page on-call immediately.
- contain:  Critical. Auto-contain + open P1 incident.
"""

def build_prompt(state: AgentState) -> str:
    s = state.silver
    ip = state.ip_reputation
    ast = state.asset_context
    idn = state.identity_context

    context = {
        "event": {
            "id":               s.id,
            "event_time":       s.event_time,
            "event_type":       s.event_type,
            "severity":         s.severity,
            "action":           s.action,
            "source_ip":        s.source_ip,
            "destination_ip":   s.destination_ip,
            "destination_domain": s.destination_domain,
            "destination_port": s.destination_port,
            "protocol":         s.protocol,
            "application":      s.application,
            "user":             s.user,
            "device_name":      s.device_name,
            "security_zone":    s.security_zone,
            "policy_violation": s.policy_violation.model_dump() if s.policy_violation else None,
            "tcp_flags":        s.tcp_flags,
            "bytes_sent":       s.bytes_sent,
            "bytes_received":   s.bytes_received,
            "process_name":     s.process_name,
            "asset_tags":       s.asset_tags,
            "cleaning_warnings": s.cleaning_warnings,
        },
        "threat_score":    state.threat_score,
        "score_breakdown": state.score_breakdown,
        "ip_reputation":   ip.model_dump()  if ip  else None,
        "asset_context":   ast.model_dump() if ast else None,
        "identity_context": idn.model_dump() if idn else None,
    }

    return f"Assess this security event and respond with JSON only:\n\n{json.dumps(context, indent=2)}"


def parse_llm_response(text: str) -> dict:
    text = re.sub(r"```(?:json)?", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group())
        raise


def reason(state: AgentState) -> dict:
    response = llm.invoke([
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=build_prompt(state)),
    ])
    parsed = parse_llm_response(response.content)
    print(parsed)

    try:
        action = ActionTier(parsed.get("recommended_action", "ticket"))
    except ValueError:
        action = ActionTier.TICKET
    
    return {
        "recommended_action": action,
        "narrative": parsed.get("narrative","No narrative generated."),
        "mitre_tactic": parsed.get("mitre_tactic"),
    }