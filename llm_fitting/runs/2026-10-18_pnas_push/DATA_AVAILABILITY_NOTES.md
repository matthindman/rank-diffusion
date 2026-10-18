# Data availability: terms and precedents (CrowdTangle, Reddit)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-18._
- **Source:** web research by a sub-agent, which read the CrowdTangle help
  pages from Wayback Machine captures and the papers' full texts. Summarised
  here in paraphrase, with only the key permission sentences quoted.
- **Owner instructions:** MODEL_STATUS §2z-dh §9 (CrowdTangle: as permissive
  as the terms allow, modelled on careful scholars) and §10 (Reddit:
  community names may be shared in analyses; the data are not redistributed).
- **Not legal advice.**

## CrowdTangle's terms (as of its shutdown)

| question | what CrowdTangle said | source |
|---|---|---|
| Raw exports (CSVs, post-level data) | May not be shared outside the university research team, and may not be shared publicly. The Terms of Service (effective 14 Oct 2020) forbid disclosing content publicly or in an external format without written consent. | Academics & Researchers FAQ, article 3323105 ([Wayback 2024-03-04](https://web.archive.org/web/20240304142528/https://help.crowdtangle.com/en/articles/3323105-academics-researchers-faq), same text in 2021); [Terms, Wayback 2023-12-23](https://web.archive.org/web/20231223040212/https://www.crowdtangle.com/terms) |
| Page or Group IDs | Explicitly allowed: "you're welcome to post the Page or Group IDs of the Pages and Groups that you studied" so that others can recreate the list. | FAQ 3323105 |
| Aggregates ("insights") | "Insights from CrowdTangle data can be shared publicly", but not the raw data. The boundary is not defined; CSVs are the only example of raw data. | "Understanding and Citing CrowdTangle Data", article 4558716 ([Wayback 2024-03-04](https://web.archive.org/web/20240304151620/https://help.crowdtangle.com/en/articles/4558716-understanding-and-citing-crowdtangle-data)) |
| Citation | Journal form: CrowdTangle Team (year accessed), CrowdTangle, Facebook, Menlo Park, California, United States, with the List ID(s). State that only public accounts are tracked and ads are excluded. | "Citing CrowdTangle Data", article 3192685 ([Wayback 2024-03-04](https://web.archive.org/web/20240304150127/https://help.crowdtangle.com/en/articles/3192685-citing-crowdtangle-data)) |
| After the shutdown (14 Aug 2024) | The notice is silent on data already downloaded. Meta Content Library (MCL) has its own rules and its own content IDs, which do not match platform IDs. MCL data stay in secure environments (Meta's, or ICPSR's SOMAR enclave). A replicator could rebuild only an approximate panel. | [Shutdown notice, Wayback 2024-06-24](https://web.archive.org/web/20240624145317/http://help.crowdtangle.com/en/articles/9014544-important-update-to-crowdtangle-march-2024); MCL documentation (developers.facebook.com, transparency.meta.com; read as extracted text, to verify before quoting) |

## Precedents (careful first)

| paper | released | withheld |
|---|---|---|
| Grunwald, Nyarko & Rappaport, PNAS 2022, doi:10.1073/pnas.2203089119 | post IDs, their own coding, code | text, interaction and share counts, subscriber counts ("restrictions from CrowdTangle") |
| Broniatowski et al., Sci. Adv. 2023, doi:10.1126/sciadv.adh2132 | CrowdTangle List IDs in the references; weekly and daily series aggregated over groups of Pages; per-Page network data under pseudonymous IDs | raw CSVs, offered to journals for verification only |
| Sangiorgio et al., PNAS Nexus 2024, doi:10.1093/pnasnexus/pgae257 (growth dynamics of news Pages; closest analogue) | Page names and URLs in the SI; aggregate distributions; fitted parameters | all per-Page series |
| Kyrychenko et al., Nat. Commun. 2024, doi:10.1038/s41467-024-52179-8 | media list with names and links; Page IDs on request | raw data |
| Li & Wagner, HKS Misinfo. Rev. 2024, doi:10.37016/mr-2020-147 | post IDs only | everything else |
| More permissive, without reasoning about the terms: Rathje et al. PNAS 2021; Marks et al. PNAS 2025; Stewart et al. PNAS 2024; Box-Steffensmeier & Moses, Sci. Adv. 2021 | post-level engagement counts | text, usernames |

**Not found:** any careful study that released per-Page interaction series
together with Page identifiers. Two Australian Dataverse deposits appear,
from file names and sizes alone, to contain raw exports. They are not models.

## What the manuscript does

- **Deposited:** code; every aggregate output; the analysis record.
- **Reddit:** results naming communities are deposited (owner, §10). The
  Pushshift archive and the community-level weekly panels are not
  redistributed.
- **CrowdTangle:** no exports and no per-Page series. Every reported
  Facebook and Instagram result is an aggregate over Pages.
- **Page IDs:** the IDs of the Facebook Pages in the study universe would be
  listed, unordered and with no values. The terms permit this. **It is
  awaiting the owner's confirmation**, because it ends the practice of never
  identifying Pages.
- **Verification:** the CrowdTangle exports and the Reddit panels are
  available to editors and reviewers on request, for verification only.
  PNAS policy requires this, and Broniatowski et al. report CrowdTangle
  allowing it. That permission was not found on CrowdTangle's own pages.
- **Citation:** the CrowdTangle citation needs the List IDs and the year of
  access, from the owner.

## Reddit

- **Reddit's 31 May 2023 statement on research access** (r/modnews; read
  through mirror sites) expects research use to be non-commercial. It asks
  researchers:
  - not to distribute Reddit data or products derived from them;
  - to credit Reddit;
  - to anonymise published results to protect user privacy.
- **Current Data API Terms** (redistribution and research use beyond rate
  limits; secondary summary, not read directly): to be checked at
  redditinc.com/policies/data-api-terms before submission.
- **Consistency with the owner's decision.** The paper names subreddits, not
  users. It reports only community-level relative activity, and it
  redistributes no data.
