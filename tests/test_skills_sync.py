"""Skill mirrors stay in sync (.claude/skills canonical, .agents/skills mirror).

The repo ships the same skill files in two locations: .claude/skills/
(read by Claude Code — canonical) and .agents/skills/ (read by other agent
tools, e.g. Codex). Divergent copies would give different models different
instructions, which is worse than either copy being stale. Edit the
.claude/skills copy, then `cp -R` the skill directory to .agents/skills.

Every file under a skill directory is compared, not only SKILL.md: skills
bundle reference files that are loaded on demand, and a stale reference in
one mirror is the same failure as a stale SKILL.md.
"""
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CANON = REPO / ".claude" / "skills"
MIRROR = REPO / ".agents" / "skills"
IGNORED = {".DS_Store"}


def _files(root: Path) -> list[Path]:
    return sorted(
        p.relative_to(root)
        for p in root.rglob("*")
        if p.is_file() and p.name not in IGNORED
    )


def test_skill_mirrors_identical():
    canon_files = _files(CANON)
    mirror_files = _files(MIRROR)
    assert any(p.name == "SKILL.md" for p in canon_files), (
        "no skills found under .claude/skills"
    )
    assert canon_files == mirror_files, (
        "skill file sets differ: "
        f"only canon={sorted(set(canon_files) - set(mirror_files))} "
        f"only mirror={sorted(set(mirror_files) - set(canon_files))}"
    )
    for rel in canon_files:
        assert (CANON / rel).read_bytes() == (MIRROR / rel).read_bytes(), (
            f"{rel} diverged between .claude/skills and .agents/skills — "
            "edit .claude/skills then cp -R to .agents/skills"
        )


def test_skill_frontmatter_is_portable():
    """Both vendors' agents load these files; keep to the shared fields."""
    for skill in sorted(CANON.glob("*/SKILL.md")):
        text = skill.read_text(encoding="utf-8")
        assert text.startswith("---\n"), f"{skill}: missing frontmatter"
        front = text.split("---\n", 2)[1]
        keys = [ln.split(":", 1)[0] for ln in front.splitlines() if ln and not ln.startswith(" ")]
        assert keys == ["name", "description"], (
            f"{skill}: frontmatter keys {keys}; use only name and description"
        )
        name = front.split("name:", 1)[1].splitlines()[0].strip()
        assert name == skill.parent.name, f"{skill}: name {name!r} != directory"
        desc = front.split("description:", 1)[1].strip().strip('"')
        assert 0 < len(desc) <= 1024, f"{skill}: description is {len(desc)} chars"
