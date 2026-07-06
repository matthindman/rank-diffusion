"""Skill mirrors stay in sync (.claude/skills canonical, .agents/skills mirror).

The repo ships the same SKILL.md files in two locations: .claude/skills/
(read by Claude Code — canonical) and .agents/skills/ (read by other agent
tools, e.g. Codex). Divergent copies would give different models different
instructions, which is worse than either copy being stale. Edit the
.claude/skills copy, then `cp -R` to .agents/skills.
"""
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CANON = REPO / ".claude" / "skills"
MIRROR = REPO / ".agents" / "skills"


def test_skill_mirrors_identical():
    canon_files = sorted(p.relative_to(CANON) for p in CANON.rglob("SKILL.md"))
    mirror_files = sorted(p.relative_to(MIRROR) for p in MIRROR.rglob("SKILL.md"))
    assert canon_files, "no skills found under .claude/skills"
    assert canon_files == mirror_files, (
        f"skill sets differ: canon={canon_files} mirror={mirror_files}"
    )
    for rel in canon_files:
        assert (CANON / rel).read_text() == (MIRROR / rel).read_text(), (
            f"{rel} diverged between .claude/skills and .agents/skills — "
            "edit .claude/skills then cp -R to .agents/skills"
        )
