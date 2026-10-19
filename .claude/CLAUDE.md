# DTFS agent workflow
Agent roster: `.claude/agents/` (dtfs-explore, dtfs-intake, dtfs-implementer, dtfs-reviewer, dtfs-integrity-auditor). Path-scoped invariants: `.claude/rules/`. No build VM, no cluster: tests run in Docker only (CLAUDE.md § A.4). `claude-mem`, `context-mode`, and `superpowers` are opt-in tools installed on this machine — none is part of the DTFS contract in CLAUDE.md.
Live-cluster incidents: use the `dtfs-diagnose` skill (`.claude/skills/dtfs-diagnose/`) with the `dtfs` MCP server (`.mcp.json`, `tools/dtfs-mcp/`); read tools are allowlisted, `op_*` always prompts.
