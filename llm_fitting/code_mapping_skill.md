# Code Mapping Skill: Visualizing Large Simulation Codebases

## Purpose

This skill provides a research-informed methodology for creating structural
visualizations and comprehension aids for large (1,500-3,000 line) scientific
simulation code files, particularly LLM-generated code requiring human review.

## Research Foundation

The methodology synthesizes findings from:

- **40 years of code comprehension experiments** (ACM Computing Surveys, 2024):
  Effective visualizations must support both *bottom-up* comprehension (reading
  code, chunking into abstractions) and *top-down* comprehension (forming
  hypotheses about purpose, then verifying).

- **CHI 2016 (Asenov, Hilliges, Mueller)**: Richer code visualizations reduce
  time to answer comprehension questions *without* causing visual overload --
  contradicting developer intuition that simpler is always better.

- **FSE 2025 -- Natural Language Outlines**: Concise prose partitioning of code
  rated "very helpful" by 63% of professional reverse engineers; 90%+
  acceptability for overall quality.

- **CHI 2024 -- ASCII Diagrams**: Programmers use ASCII diagrams as
  "professional artifacts across many steps in the development lifecycle."
  Text-based diagrams are version-control friendly, render universally, and
  have near-zero tooling overhead.

- **C4 Model (Simon Brown)**: The most widely adopted architecture diagramming
  framework. 2025 State of Software Architecture survey: used by 79-81% of
  respondents. Core principle: *multiple diagrams at different abstraction
  levels*, each answering one question.

- **CACM "Software Development with Code Maps"**: Box-and-arrow diagrams are
  by far the most common representation developers draw; each box = a software
  entity, each arrow = a relationship.

- **Meta-study on algorithm visualization (Georgia Tech)**: *Active engagement*
  with visualizations is more effective than passive viewing. Creating or
  annotating diagrams produces stronger comprehension than merely reading them.

## Recommended Visualization Stack

For a ~2,000-line scientific simulation file, produce these artifacts in
priority order. The top two are essential; the third is recommended.

### 1. Annotated Structural Outline (ESSENTIAL)

**What**: A hierarchical table of contents with line ranges, function
signatures, key data flows, and brief narrative for each section.

**Why**: Highest comprehension value per unit effort. Supports both top-down
(scan the outline to form hypotheses) and bottom-up (find the code section you
need) strategies. Research shows 90%+ acceptability.

**Format**: Markdown with hierarchical headers, line references, and inline
annotations of data flow using arrow notation (`raw_data --> parameters`).

**Guidelines**:
- Group functions by *functional role*, not lexical order
- Include line ranges (e.g., `lines 57-136`) for navigation
- Annotate each section with its primary data inputs and outputs
- Mark key mathematical formulas with `[MATH]` tags
- Mark critical assumptions with `[ASSUMPTION]` tags
- Note coupling points between sections

### 2. Data Flow Diagram (ESSENTIAL)

**What**: A diagram showing how data transforms through the pipeline: raw
data --> cleaned panel --> empirical targets --> estimated parameters -->
simulation state --> diagnostics --> validation.

**Why**: For scientific simulation code, understanding data transformations
*is* understanding the model. Research on combined control-data flow
approaches (CDFGs) shows these generalize single-aspect models. This is the
single most important diagram for a scientific codebase.

**Format**: Mermaid flowchart (renders natively on GitHub) with:
- Subgraphs for major pipeline stages
- Annotated edges showing variable names
- Color coding: blue for data, green for parameters, orange for outputs
- Node shapes: rectangles for processes, parallelograms for data stores,
  rounded rectangles for decision points

**Guidelines**:
- Keep to one screen height if possible (max ~40 nodes)
- Use subgraphs to cluster related operations
- Show the "main trunk" prominently; secondary flows as branches
- Label edges with actual variable names from the code
- Include line numbers in node labels for navigation

### 3. Call Graph with Functional Clustering (RECOMMENDED)

**What**: A directed graph where nodes are functions and edges are calling
relationships, with nodes grouped into clusters by functional area.

**Why**: Reveals the actual execution structure; exposes coupling between
functional areas; identifies the central "hub" functions. For 30-60
functions, a clustered call graph remains readable.

**Format**: Graphviz DOT when available (superior auto-layout for complex
graphs), or matplotlib with manual layout as a fallback (see "Rendered
Diagram Best Practices" below). Either way:
- Top-to-bottom layout (`rankdir=TB` in DOT) for pipeline-style code
- Cluster subgraphs matching the structural outline sections
- Node color coding by functional area
- Edge styles: solid for direct calls, dashed for callback/closure calls
- Node labels include `name (lines X-Y)`

**Guidelines**:
- In Graphviz, use `compound=true` to show inter-cluster edges cleanly
- Highlight the simulation kernel as the central hub
- Mark entry points (main, top-level script) distinctly
- Keep cluster count to 4-7 (matches cognitive chunking limits)
- When using matplotlib manual layout, route caller arrows to different
  x-positions on the hub node to avoid overlap (see anti-pattern:
  "fan-of-arrows")

## Supplementary Visualizations (Optional)

### 4. Simulation Kernel Control Flow

For the most complex single routine (typically the simulation kernel / time
loop), create a dedicated control flow diagram showing:
- The time-step loop structure
- Conditional branches (heavy tails, ARCH, rank-dependent options)
- State variable updates in order
- Exit/entry mechanics

### 5. Function Complexity Table

A simple table listing each function with:
- Line count
- Parameter count
- Number of callees
- Cyclomatic complexity estimate (branch count)

Sort by complexity descending. This directs review effort to the
highest-risk routines.

## Design Principles

1. **One diagram, one question**: Each visualization answers a specific
   comprehension question. Do not combine everything into one mega-diagram.

2. **Multiple abstraction levels**: Provide at least a high-level overview
   (structural outline + data flow) and a detailed view (call graph or
   control flow) -- matching the C4 model philosophy.

3. **Consistent visual language**: Use the same colors, shapes, and naming
   conventions across all diagrams in a set.

4. **Keep diagrams near code**: Store as Mermaid blocks in markdown files
   alongside source code. Version-control friendly, reviewable in PRs.

5. **Include line references**: Every node in every diagram should reference
   the source line range, enabling direct navigation.

6. **Prefer text-based formats**: Mermaid and DOT are diffable, LLM-
   generatable, and render without special tooling. Avoid binary formats.

7. **Scope ruthlessly**: For a 2,000-line file, the structural outline covers
   everything; diagrams should focus on the critical 30-40% of the code
   (typically the estimation pipeline and simulation kernel).

8. **Plan for iteration**: Rendered diagrams (PNG/SVG) almost never come out
   readable on the first pass. Budget for at least two rounds of
   generate-inspect-fix. Inspect the actual rendered output, not just the
   code that produces it.

## Rendered Diagram Best Practices

When generating PNG/SVG diagrams (e.g., via matplotlib), text-based formats
like Mermaid handle layout automatically, but rendered diagrams require
explicit coordinate management. These rules prevent the most common failures:

### Canvas and Typography

- **Canvas size**: Start at 2× what seems necessary. A 6-section pipeline
  diagram for a 2,000-line file needs at least 24×30 inches at 150 DPI. An
  18×22 canvas will produce unreadable cramming.
- **Font size minimum**: 10pt for body text in boxes, 11–12pt for important
  labels (function names, section titles), 9pt absolute minimum for
  secondary annotations. Anything below 9pt is illegible when the diagram
  is viewed at normal zoom.
- **Box padding**: Boxes need generous internal padding *and* generous
  spacing between them. As a rule of thumb, the gap between adjacent boxes
  should be at least 50% of the box height.
- **DPI**: 150 DPI is sufficient for screen viewing and print. Higher DPI
  increases file size without meaningful quality improvement for diagrams.

### Arrow Routing

- **No fan-of-arrows**: When N data flows feed into one target, do NOT draw
  N separate arrows converging on a single point. This creates an
  unreadable tangle. Instead, use a **collector bar** (a horizontal line
  that all sources feed into, with a single arrow out to the target) or
  route each arrow to a different connection point on the target box.
- **Label ambiguous arrows**: If an arrow's meaning isn't obvious from its
  source/target positions alone, add a text label. Curved long-distance
  arrows especially need labels.
- **Avoid crossings**: Route arrows to different sides of target boxes.
  When crossings are unavoidable (e.g., feedback loops), use dashed lines,
  distinct colors, or curvature to distinguish the crossing arrow from
  the arrows it crosses.
- **Color-code by category**: Use the same color for an arrow as its
  source section (e.g., estimation arrows in green, simulation in orange).
  This makes it trivial to trace flows visually.

### Matplotlib-Specific Notes

When Graphviz is unavailable, matplotlib with `FancyBboxPatch` and
`ax.annotate` (arrowprops) is a workable alternative:
- Use axis coordinates in data units (not normalized 0–1), with `set_xlim`
  / `set_ylim` matching the figure inches. This makes coordinate math
  intuitive (1 unit ≈ 1 inch).
- Use `FancyBboxPatch` with `boxstyle="round,pad=0.3"` for rounded boxes.
- Use `ax.annotate` with `arrowstyle='-|>'` and `connectionstyle='arc3,rad=X'`
  for curved arrows. Non-zero `rad` values prevent overlapping parallel
  arrows.
- Set `shrinkA` and `shrinkB` (4–6 points) so arrowheads don't overlap
  box borders.

## Anti-Patterns

- **The mega-diagram**: A single diagram with 100+ nodes is unreadable.
  Split by abstraction level or functional area.
- **Fan-of-arrows**: Multiple arrows converging on a single point. The
  result is always an illegible tangle. Use collector bars or staggered
  connection points instead.
- **Tiny fonts**: Anything below 9pt in a rendered diagram. If you need to
  shrink text to fit, the canvas is too small — increase the figure size
  instead of reducing font size.
- **Insufficient whitespace**: Cramming boxes edge-to-edge. Whitespace is
  not wasted space; it is what makes the structure legible.
- **UML-only**: Research consistently shows UML is too abstract for working
  programmers. Embed actual variable names, line numbers, and formulas.
- **Diagram without text**: Diagrams complement but never replace structured
  prose. Always pair with the annotated outline.
- **Static screenshots**: Use text-based diagram formats (Mermaid, DOT) that
  can be regenerated and version-controlled.
- **Over-decoration**: Avoid 3D effects, gradients, or decorative elements.
  Use color sparingly and only for semantic meaning.
- **Single-pass rendering**: Assuming a rendered diagram will be correct on
  the first attempt. Always inspect the actual PNG output and iterate.
