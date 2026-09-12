# graphify
- **graphify** (`.claude/skills/graphify/SKILL.md`) - any input to knowledge graph. Trigger: `/graphify`
When the user types `/graphify`, invoke the Skill tool with `skill: "graphify"` before doing anything else.

# DTFS agent workflow
Agent roster: `.claude/agents/` (dtfs-explore, dtfs-intake, dtfs-planner, dtfs-implementer, dtfs-reviewer, dtfs-integrity-auditor, dtfs-release-case-author). Path-scoped invariants: `.claude/rules/`. `claude-mem`, `context-mode`, and `superpowers` are opt-in tools installed on this machine — none is part of the DTFS contract in AGENTS.md.
