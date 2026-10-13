# External evidence on the Reddit and Facebook features of M2: report

_Web research, 2026-10-13 to 2026-10-14 (the session ran across midnight), Claude (Opus 5.5), working as a research sub-agent. An evidence review, not an argument. The sub-agent could not write this file itself; the text was returned to the calling agent to save._

_Saved unchanged by the calling agent (Claude, lead session) on 2026-10-14 from the sub-agent's returned text. The calling agent read it in full and did not open its sources; see MODEL_STATUS §2z-bz for what is and is not verified._

**What was and was not done.** Web search and page fetches only. No project data folder was opened, listed or searched; no repository code was run; git was not used; the only project file read was `llm_fitting/runs/2026-10-13_M2_entrants_shape/REPORT.md`. No existing file was edited.

**How sources were reached.** `reddit.com`, `redditinc.com` and `web.archive.org` are blocked for the fetch tool. Reddit's own posts were therefore read in one of two ways, and each source below says which: (a) an Internet Archive (Wayback Machine) copy opened in the in-app browser, or (b) the text of the post as stored by the public Arctic Shift archive API (`arctic-shift.photon-reddit.com`), which re-serves archived Reddit posts and comments. Route (b) is an archive copy of a primary source, not the live page.

**One thing here is not "found on the web".** Section D contains a spot check that I ran myself against the Arctic Shift API: for a few hundred archived items at chosen instants in 2018-12 to 2022-12, I read the creation time, the retrieval time and the score, and nothing about which subreddit they came from. It touches only the period of the project's development panels (nothing after 2022-12-31), uses no project file, and is labelled wherever it is used. The samples are small (100 items per instant). Treat it as a pointer to a check the project should repeat on its own files, not as a result.

**Labels.** *Primary* = the organisation's or author's own statement. *Secondary* = reporting about it. *Community* = statements by informed users (r/pushshift regulars, moderators), not by the maintainer. *Anecdote* = individual user reports. *Unconfirmed* = I did not open a source that states it.

Direct quotation is kept to one short phrase in this file; everything else is paraphrase, with a source key after each claim. Source keys resolve in section 5.

---

## 1. Summary table

| # | Measured feature | Best-supported external explanation(s) | Strength | Is an archive artifact a live possibility? |
|---|---|---|---|---|
| 1 | Level of the top K up about half a log unit 2019→2020, slow afterwards (flat on submissions from mid-2021); lower ranks grow faster | Platform-wide growth in 2020, then a stall in votes. Reddit's own totals: upvotes +53.8%, posts +52.4%, daily users +44% in 2020; in 2021 posts +19%, comments +12%, upvotes +1%; in 2022 posts +14%, comments +7% [R4, R5, R6]. Its IPO filing says user growth rose during the pandemic and then slowed or fell [R24]. For the tilt toward lower ranks: a January 2020 home-feed change that deliberately boosted small and medium communities [R7], and Reddit's June 2022 statement that its 2021 feed work led to more small subreddits gaining subscribers [R21c]. | **Strong** for the 2019→2020 jump and for the stall in votes in 2021. **Moderate** for "lower ranks grow faster" (direction documented, size not). | **Low.** By spot check, scores in the archive were captured days to months after posting in both 2019 and 2020, so the jump is not a capture-delay effect (D). One confirmed hole: submissions of 28–30 April 2020 were captured within hours or seconds and carry scores near 1 (D4). |
| 2 | 5–10% of the top K each year are subreddits with no earlier archived activity; exits almost never vanish | Event-driven new communities are documented by name for 2020 and 2021 (pandemic, unemployment, protest, meme-stock communities) [R4, R5]. Reddit removed about 745,000 dormant subreddits in June–July 2021, freeing their names for re-creation [R13a, R13c]. | **Moderate** that many successful new communities were created; **none found** for a count of subreddits created per month or year. | **Possible, minor.** (i) After the June–July 2021 purge a re-created name can look new. (ii) The archive cannot re-collect content from subreddits banned before its re-collection pass, so banned communities can vanish retroactively (D5). Neither produces a 5–10% inflow. |
| 3 | Rank 1's share of submission karma: up to about 10% by mid-2020, about 8% in spring 2021, about 4% in the quarter from 28 June 2021, 1.2% by late 2022 | The date is real: on 28 June 2021 Reddit moved every iOS and Android user to a machine-learned, personalised Best sort for the Home feed, replacing a vote-and-age ranking, and replaced community suggestions with recommended posts [R1, R2]. Reddit said the new sort showed users posts from more communities [R1, admin comment]. Decline before the date: tests and partial exposure are documented or reported from mid-2020 onward (B3). Decline after: Discover tab (Feb 2022) and the steps that made Best the default and then the only Home sort (June and December 2022) [R21]. | **Strong** that a relevant change happened on that date. **Moderate** that it explains the step: the direction matches Reddit's description, but I found no external measurement of top-subreddit shares before and after. | **Live but not supported by what I found.** The date falls three days before a boundary between two archive files that were collected very differently (posts of late June 2021 were re-collected about 20 days after posting; posts of July 2021 about 360 days after). Both delays are long, so a score-truncation artifact is not indicated. A year-later re-collection could differ in other ways (removed or deleted posts, bans). A daily series would separate 28 June from 1 July (section 4). |
| 4 | Ranks 250, 500, 1,000 step up 10–14% in share in the same quarter and stay | Same change as feature 3. Reddit described the new sort as surfacing communities users had joined but seldom visited, and recommending posts from similar communities [R1, R2]; in 2022 it credited the 2021 feed work with growth in smaller subreddits [R21c]. | **Moderate.** | Same as feature 3. |
| 5 | Rank 1's share of comment karma falls steadily 12% → 4%; top five step down in spring 2021 | Steady fall: growth from below (feature 1) plus the January 2020 home-feed change, for which Reddit reported commenters moving from the largest communities to small and medium ones [R7]. Spring 2021 step: candidates are the end of the WallStreetBets surge of January–March 2021 [R17, R24], a January 2021 test that changed how comments are ordered within threads [R12c], and February 2021 notification changes that widened recommended communities from a few hundred to several thousand [R12e]. | **Moderate** for the steady fall; **weak** for the spring 2021 step (candidates with the right dates, nothing that ties them to the top five). | **Low.** By spot check comment scores were captured 3 to about 320 days after posting at every sampled instant, with no short-delay months (D4). |
| 6 | Chart caption "June 28, 2021 algorithm change" | Confirmed from Reddit's own post of 22 June 2021 [R1] and its follow-up of 13 July 2021 [R2]. Scope: Home feed, Best sort, iOS and Android only. An admin stated it did not affect r/all, r/popular or the other sorts [R1, admin comment]. | **Strong** (primary). | Not applicable. |
| 7 | Facebook: interactions fall at all ranks together, about 0.17 log units a year, Dec 2020 to mid-2022 | Meta's reduction of political content in Feed (tests from 10 February 2021, widened through 2021, global by 19 July 2022) [F1]; the 2022 shift toward recommended content from unconnected accounts [F5, summary only]. Independent CrowdTangle-based counts on US political and news pages show larger falls (about −30% in 2021, about −37% in 2022) [F3]. | **Moderate** that page interactions fell for platform reasons; the size for a general page list is not externally measured. | **Possible.** CrowdTangle lost staff in 2021 and stopped taking new users in January 2022 [F6, summary only]; a documented incident removed posts from the tool for months in 2021 [F4]. A fixed page list also decays on its own. |

**Which subreddits held the top.** Not found for 2019–2021. For 2022, a media-analytics firm's monthly rankings (secondary) put r/memes first by post score in January to June except April, when r/place led (and in July, per a search summary I did not open), with r/MadeMeSmile and r/shitposting next; r/AskReddit led by number of comments [O1]. Reddit's own recaps rank by views or by posts plus comments, not by score [R3–R6].

---

## 2. Findings by candidate explanation

### A. Entry and growth

**Found (primary, Reddit's year-end posts):**

| Year (as of) | Users | Posts | Comments | Upvotes | Active communities | Source |
|---|---|---|---|---|---|---|
| 2019 (October) | 430 million monthly, +30% | 199 million | 1.7 billion | 32 billion | more than 100,000 | R3 |
| 2020 (as-of date not seen in the text I read; a search summary says 6 November) | 52 million daily, +44% | 303.4 million, +52.4% | 2 billion, +18.6% | 49.2 billion, +53.8% | — | R4 |
| 2021 (9 November for comments and votes) | — | 366 million, +19% | 2.3 billion, +12% | 46 billion, +1% | more than 100,000 | R5 |
| 2022 (20 November for comments and votes) | — | 430+ million, +14% | 2.5+ billion, +7% | 24+ billion (no change given) | more than 100,000 | R6 |

- The 2020 rise in votes (+53.8%, about 0.43 log units) is close to the measured rise in the level of the top K (about half a log unit). The 2021 figure for votes (+1%) matches the measured flattening on submissions.
- Reddit's IPO filing (February 2024) says its user base grew faster during the pandemic and that growth then slowed or reversed as the pandemic's effects faded; it names WallStreetBets traffic in the first quarter of 2021, and the Ukraine war and a game release in the first quarter of 2022, as peaks followed by declines [R24].
- Reddit named new communities created in 2020 as among the largest in their categories (pandemic-recession, stimulus and police-brutality communities) and reported very large activity rises in existing ones (unemployment, Black Lives Matter) [R4]. For 2021 it reported one meme-stock community's subscribers up by a factor of about nine thousand [R5].
- Requests to adopt unmoderated subreddits: 31,239 in 2019 (+81% on 2018); 23,520 in the first half of 2020; 23,536 in the second half; 40,103 in 2021 [R23, R13c]. A weak proxy for interest in running communities; it peaks in 2020.
- August 2020: Reddit began testing limits on how fast one account can create communities, to deter spam and name squatting [R14].
- June–July 2021: Reddit removed about 745,000 dormant subreddits (354,958 with no posts ever, about 390,000 with at most 100 posts and none in the prior year), announced 16 June, started 23 June. The names became available to re-create [R13a, R13b, R13c].
- Late August – September 2021: a test prompting users to create a new community from a popular post [R12i].

**Evidence against, or limits:**

- Reddit's totals are not a consistent series. The 2021 post gives 46 billion upvotes and calls it a 1% rise, though the 2020 post gave 49.2 billion; the 2022 post gives 24+ billion with no comparison [R4, R5, R6]. Either the windows or the definitions differ from year to year. The apparent halving in 2022 is unexplained (see C).
- The figure of more than 100,000 active communities is repeated unchanged in 2019, 2021, 2022 and in the 2024 filing [R3, R5, R6, R24]; it carries no information about growth in the number of communities.

**Not found:**

- Any count of subreddits created per month or per year for 2017–2023, from Reddit or from a paper. Searched: Reddit's recaps and filing, moderator-news posts, the Pushshift dataset paper, several arXiv papers on Reddit's growth, statistics sites. One admin post refers to a chart of subreddits created per day during the 2021 purge, but it is an image I could not read [R13c].
- The quarterly table of daily active users in the IPO filing. The fetch tool returned only part of the document and not that section.
- Any published study of a pandemic-era surge in subreddit creation as such. Papers found study particular topics or communities.

### B. Feed and recommendation changes

**B1. The registered date is confirmed.** Reddit's post of 22 June 2021 [R1, primary, read in a Wayback copy] announced, in its words, "Starting on June 28, all mobile users" would get a new, more personalised Best sort for the Home feed. From that post and its admin comments:

- Before: Best used upvotes, downvotes, post age and how much time the user spent in a subreddit.
- After: a learned model predicts, per user and post, the chance of viewing, commenting, voting, subscribing or unsubscribing; posts are then drawn by weighted random sampling on a combined score, with an explicit step to keep the feed diverse and to move similar posts apart.
- Candidates come from the user's subscriptions, communities similar to them, topics chosen at sign-up, recently visited communities, and popular or locally popular posts.
- Community suggestions in the feed (in use since 2017) were replaced by recommended posts labelled with the reason.
- Platforms: iOS and Android. An admin replied that r/all, r/popular and the other Home sorts were not changed (comment of 23 June 2021).
- An admin replied that in their statistics users were seeing content from more communities under the new sort, and that it was surfacing communities users had joined but spent little time in (comments of 22 June 2021).
- The post cites experiment results (views of post pages up 5.4% per user, comments up 4.4% per user) and describes slow rollouts, so some users had the new sort before 28 June. It does not say from when.

The follow-up of 13 July 2021 [R2] states that the new sort was by then live for everyone on iOS and Android. Trade coverage is consistent [O2].

**B2. Changes aimed at smaller communities, or at spreading attention, with dates:**

| Date | Change | Source |
|---|---|---|
| Nov 2019 – Jan 2020 | Home-feed ranking (Best sort) changed to give a small boost to small and medium communities; experiments from November, rolled out in the week before 14 January 2020. Reported effect: commenters in small and medium communities +10%, commenters in the largest −0.3%, commenting sitewide +0.4%. Small was defined by weekly posts and comments. r/popular and r/all unchanged. | R7 (primary) |
| 5 Aug 2020 | Best sort added to r/popular as its default, rolling out by platform; Reddit said it would experiment there with, among other things, a more diverse set of content. | R8 (primary) |
| 23 Oct 2020 | Announced a running series of experiments on how posts are consumed, sorted and voted on (new Reddit and apps). | R10 (primary) |
| 15 Dec 2020 | Tests: comments shown inside feeds on Android; finer topic choices at sign-up to build a new user's Home feed. | R12a (primary) |
| 5 Jan 2021 | Tests of new kinds of trending notifications. | R12b (primary) |
| 2 Feb 2021 | Test: newly joined small communities featured more often in notifications for one to two weeks, because their posts compete poorly for feed space. | R12d (primary) |
| 11–17 Feb 2021 | Sexually explicit communities removed from r/all (announced 11 February, effective the following week). | R11 (primary) |
| 17 Feb 2021 | Notification recommendations use more signals; for users with trending and recommendation notifications on, the pool of recommended communities widened from a few hundred to several thousand. | R12e (primary) |
| 13 Apr 2021 | Test: fewer push notifications for users who had received many. | R12f (primary) |
| 17 May 2021 | Engineering post describing the personalised recommendation system as built through successive experiments. | R22 (primary) |
| 22 Jun 2021 | Discover tab test announced for 10% of iOS users later that month; r/trendingsubreddits retired. | R12g (primary) |
| **28 Jun 2021** | **New Best sort for Home on iOS and Android (B1).** | R1, R2 (primary) |
| 4 Aug 2021 | Discover tab test extended to Android. | R12h (primary) |
| 24 Feb 2022 | Discover tab launched in the apps. | R21a (primary) |
| 13–21 Jun 2022 | Home sort control moved into Settings and Home defaults to Best; sorting removed from r/popular, which defaults to Hot. | R21b (primary) |
| 15 Dec 2022 | Home sort control removed on iOS (Android to follow in early 2023); Home is Best, with a separate Latest feed. Reddit said the 2022 changes raised the number of posts users saw in Home. | R21e (primary) |

On 2 June 2022 Reddit wrote that its 2021 feed work, naming the Best-sort update, had helped more users find and join communities and that more small subreddits had seen subscription growth [R21c, primary]. That is the company's own statement of the direction of the effect. It gives no figures.

**B3. Evidence that exposure began before 28 June 2021** (relevant to the top having stopped growing in mid-2020 and falling from late 2020):

- Reddit's own post refers to experiments and slow rollouts before the date [R1].
- *Anecdote.* r/help: a user on 28 January 2021 reported that the app's Home feed under Best was mostly posts from communities they had not joined, and that switching to Hot removed them [A1]. Similar titles recur in March, April, May and June 2021; on 3 June 2021 a user asked how to turn off feed items labelled as similar to a community or as popular on Reddit, which are the labels of the new recommendations [A1]. r/TheoryOfReddit, 13 June 2021: a user reported that for some weeks Best had been showing many posts with zero or one vote [A2].
- These are individual reports found by title search in one help forum. They show that some users saw post recommendations in Home months before the announced date; they do not show how many.

**B4. Evidence against the Best-sort explanation of features 3–4, or limits to it:**

- Scope. The change covered one sort on one feed on two platforms. Desktop, old Reddit, third-party apps, r/all, r/popular and community pages were not changed [R1]. I found no figure for the share of votes cast from the mobile Home feed.
- Timing. The measured decline starts a year earlier. Something else, or earlier exposure (B3), has to account for mid-2020 to mid-2021.
- The January 2020 small-community boost did not stop the first rank's share from doubling during 2020. Feed tweaks of that size are evidently not decisive on their own.
- Reddit reported higher engagement per user from the new sort, not lower [R1]. A halving of the first rank's share therefore has to be a reallocation, and no source measures that reallocation.
- *Anecdote.* A July 2021 thread asks why large animal-picture communities had dropped off the front page; replies disagree, and offer personalisation, the new Home recommendations, a change in mood, and bans of karma-farming accounts as causes [A3].

**Not found:** any Reddit statement of how much attention moved between large and small communities after 28 June 2021; any date for the new sort reaching the website (by December 2022 Reddit mentions recommendation feedback on the website [R21e]); any other feed change dated within a week of 28 June 2021 beyond those in the table.

### C. Vote and score mechanics

**Found:**

- No documented change in 2019–2022 to how a post's or comment's score is computed, capped or fuzzed. The Pushshift paper notes only the standing fact that Reddit fuzzes scores [P1].
- 6 July 2020: experiment giving users karma for awards given and received; a follow-up post on award karma is dated 24 September 2020 (title only). This changes user karma totals, not the score of a post or comment [R9].
- 19 January 2021: A/B test in which the Best sort *within comment threads* favours comments with a high upvote ratio, to help good comments with few views [R12c]. This changes which comments collect votes inside a thread; it could shift comment karma but there is no reason to expect it to shift it between subreddits.
- 29 September – 13 October 2021: voting and commenting on posts older than six months allowed unless moderators opt out; in the pilot, archived posts added 2.86% to votes in the participating communities [R15]. Small, and it acts on old posts.
- Enforcement against what Reddit calls content manipulation (spam, vote manipulation, brigading) rose through 2021: account sanctions 4.8 million in the first quarter, 8.2 million in the second, 11.0 million in the third; total admin removals in 2021 up 27% on 2020 [R20]. Circumstantial: if automated votes or repost accounts were concentrated in the largest image communities, enforcement would lower their summed scores without any change in human attention. I found no source that says so.

**Unexplained:** Reddit's own count of upvotes roughly halves between its 2021 and 2022 year-end posts (46 billion as of 9 November 2021; 24+ billion as of 20 November 2022) while posts and comments rise [R5, R6]. Neither post explains it and I found no coverage that does. It may be a change of definition. The project's submission totals do not show a halving in 2022 (M2 reports the platform total roughly flat), so the two are not measuring the same thing, or one of them changed.

**Not found:** any announcement of a change to vote fuzzing, score display or score caps in the period. Searched r/changelog, r/announcements, r/blog, r/reddit, r/modnews and r/redditsecurity titles for 2019–2022 and read the candidates.

### D. Archive collection

**D1. What the maintainer said (primary, Jason Baumgartner, u/Stuck_In_the_Matrix):**

- The dataset paper defines `retrieved_on` as the time an item was crawled and says scores are fuzzed. It does not say when scores are captured relative to posting or how the monthly files are built [P1; pages 1–6 read].
- 10 February 2019: the key `retrieved_on` would eventually be renamed `retrieved_utc` [P10].
- 27–29 March 2019: comments and submissions are re-scanned about 24 hours after posting to update scores; the monthly re-collection of the previous month is the long-term update and gives values very close to final [P3].
- 2 July 2019: items are scraped twice after the first scrape; the last time is when the monthly files are started, typically a few days to a week after the month ends, when most scores are within about 1% of final [P4]. In the same thread a user identified by others as a Reddit data scientist said a little under 95% of comments are within 1% of their final score after 24 hours [P4].
- 1 August and 18 September 2019: building a monthly file includes a second pass to fetch missing ids; spam was slowing this and the files were months late [P5].
- 3 June 2021 (tweet relayed in r/pushshift; only the first part is preserved): files for all of 2020 and early 2021 were coming; he had expected to have to re-collect those months after a server failure but found a backup server had kept collecting [P6a]. *The rest of the tweet could not be retrieved*, so what exactly went into those files is unconfirmed from his words.
- 3–5 August 2021: submission files complete through June 2021; 2021 comment files being produced [P6b, P6c]. 29 August 2021: first half of 2021 comments being uploaded [P6d].
- 2 May 2022: the monthly files had fallen behind and he was re-collecting the earlier months [P7a]. 16–23 June 2022: catch-up release; comment files for July–December 2021 available 23 June 2022, 2022 to follow [P7b, P7d]. Files through June 2022 in a torrent by 17 July 2022; through December 2022 by 14 January 2023 [P7e, P7f].
- 19–24 June 2022: quarantined subreddits are collected only if explicitly opted into, and content of banned subreddits cannot be recovered [P7c; both points as explained by the torrent maintainer, community].

**D2. What informed users said (community):**

- The monthly file is a fresh collection made when the file is built, not a copy of what the live service holds; its retrieval-time field shows when [P6b, comment of 3 August 2021].
- The files contain data retrieved at least 30 days later (7 September 2021) [P9c]; a November 2018 comment carried a retrieval time 44 days later in the monthly file [P9d].
- Scores served by the live Pushshift service were mostly 1; the monthly files are the exception and have updated scores [P14; P11].
- 14 November 2022: a user measured the October 2021 submission file and found retrieval 235 to 267 days after creation, 251 on average [P8a]. 18 November 2022: a post in the October 2022 file had been retrieved about 20 days after creation, while the live service showed seconds; users concluded the files hold re-collected items with updated scores [P8b].
- The torrent maintainer (u/Watchful1), 1 May 2024: across Reddit's history items were collected by different methods; for large stretches they were read close to creation and never updated, so the score is 1; in other stretches a second scan 24 or 36 hours later updated it. He did not say which years are which [P9b]. On 11 September 2021 he believed the files generally ran at least several days behind [P9a].

**D3. Gaps and outages on record:** ingest outages or lags in June, July, September and October 2020, and an outage ending 8 February 2021 [P15, titles only]; 20 August 2021, a large hole in submission ids attributed to mass deletion [P15, title only]. These concern the live service. Because the monthly files are re-collected by id, an outage of the live service does not by itself leave a hole in a monthly file. For earlier years, an audit found missing items clustered in time and by community, but it covers data only to mid-2017 [P13].

**D4. Spot check of capture delay (mine; small samples; see the note at the top).** The Arctic Shift project states that its files for March 2023 and earlier are the Pushshift files, unmodified by its own later collection changes [P12a]. Its API returns the retrieval-time fields. For each instant I took up to 100 items created within a few minutes (submissions) or 40 seconds (comments) and computed retrieval time minus creation time. Two independent community measurements agree with what I got (October 2021 submissions: 251 days on average in P8a, 253 days in mine; an October 2022 submission retrieved 9 November 2022 in P8b, same batch date in mine). I cannot verify that the project's copy of the files is identical to what this API serves.

*Submissions, by creation date:*

| Created | Retrieved | Delay | Field present |
|---|---|---|---|
| Dec 2018 – Aug 2019 | month by month, Jan – Aug 2019 | 9 to 87 days at mid-month; about 1–2 days in the last week of August 2019 | `retrieved_on` |
| Sep 2019 – 27 Apr 2020 | one pass, 5–28 Apr 2020 | from 203 days down to about 1 day | `retrieved_on` |
| **28 Apr 2020** | 28 Apr 2020 | **5 to 16 hours** | `retrieved_on` |
| **29–30 Apr 2020** | at ingest | **seconds; 84–91% of sampled scores are 0 or 1, maximum 6** | `retrieved_on` |
| May 2020 – 12 Jun 2021 | one pass, 4–13 Jun 2021 | from 399 days down to hours (**12 Jun 2021: 4 to 14 hours**) | `retrieved_utc` |
| 13 – about 19 Jun 2021 | 24 Jun 2021 | 7 to 11 days | `retrieved_utc` |
| 20–30 Jun 2021 | 20 Jul 2021 | 19 to 30 days | `retrieved_utc` |
| **1 Jul – Dec 2021** | **one pass, 24–26 Jun 2022** | **360 days down to about 180** | both |
| Jan – 29 May 2022 | one pass, 30 May – 2 Jun 2022 | 138 days down to about 1 day | both |
| 30 May – Jun 2022 | 24 Jun and 6 Jul 2022 | 5 to 25 days | both |
| Jul – Dec 2022 | about 8–12 days after month end (November: 8 Jan 2023) | 8 to 26 days (November: 39 to 54) | `retrieved_on` |

*Comments, by creation date:* December 2018 to May 2019, 24 to 54 days; June 2019 to July 2020, about 69 to 108 days; August 2020 to January 2021, about 120 to 145 days; February to June 2021, retrieved 22 July to 7 August 2021 (29 to 157 days); July to November 2021, retrieved February to May 2022 in reverse order (88 to 320 days); December 2021 and February to May 2022, retrieved 15–23 June 2022; January 2022, 19 to 37 days; June to December 2022, 3 to 30 days. The shortest delay seen was about one day, for a minority of items in April 2019. Fields: `retrieved_on` through January 2021, `retrieved_utc` for February to June 2021, `retrieved_on` from July 2021.

What this says about the features:

- **2019 against 2020 (feature 1).** Delays are long in both years. The level jump is not a capture-delay effect.
- **The 28 June 2021 quarter (features 3–4).** The delay changes sharply at the file boundary of 1 July 2021 (about 19 days before it, about 360 after), which is three days after the registered date and inside the same week. But 19 days is already long enough for scores to have settled, by the maintainer's own statements and the statement of the Reddit data scientist in that thread (D1). The share of sampled scores at 0 or 1 and the median score do not move across the boundary in my samples. So a score-truncation artifact is not indicated. What I cannot rule out from outside: that items re-collected a year later differ in some other way (posts later removed or deleted, communities banned or made private in the interval).
- **Comments (feature 5).** No short-delay period found; the 1 July 2021 boundary shows the same jump in delay (29 to about 320 days), again long on both sides.
- **Confirmed short-delay pockets in submissions**, where summed scores are biased down for every subreddit: 28–30 April 2020 (the last two days essentially zero); 12 June 2021 (hours); about 29 May 2022 (about a day); the last week of August 2019 (one to two days). The weekly panel's week of 27 April 2020 should show a dip of roughly a third in submission karma across the board.
- **A practical point.** The retrieval-time key changes name across files (`retrieved_on`, then `retrieved_utc`, then both, then `retrieved_on` again). Code that reads only one key will see missing values for May 2020 to June 2021 submissions and February to June 2021 comments.

**D5. Coverage.** Collection is by id sequence and is not selective by community size [P5, P8]. Exceptions: quarantined communities need an explicit opt-in, and banned communities cannot be re-collected [P7c]. With re-collection passes that ran up to a year after posting, a community banned in the interval may lose its last months from the files, unless the files were built from data kept at first ingest (the June 2021 tweet leaves this open). Two dated bans fall inside such intervals: 29 June 2020 (about 2,000 communities; files for May–June 2020 were collected in June 2021) and 1 September 2021 (r/NoNewNormal; files for July–August 2021 were collected in June 2022) [R16, R19b]. This is my inference from the dates; I did not find anyone documenting it.

**Not found:** any statement by the maintainer of the exact collection schedule for 2020–2022; any paper auditing completeness or score accuracy of the 2019–2022 files; any documented change in coverage of small or new subreddits; the full text of the June 2021 tweet about the backup server. One paper is reported, in a search summary I did not verify against its text, to have found Pushshift scores too low for some WallStreetBets posts and an outage in early February 2021 [P16]; that concerns the live service.

### E. Dated events that move specific large communities

| Date | Event | Source |
|---|---|---|
| 29 Jun 2020 | Content-policy change; about 2,000 subreddits banned, about 200 of them with more than 10 daily users, including r/The_Donald and r/ChapoTrapHouse. | R16 (primary) |
| 24–30 Jan 2021 | WallStreetBets: tenfold rise in new users viewing it; subscribers +5.6 million in two weeks; site peak traffic up more than 35% in a day; sign-ups 2.6 times the prior week; a matching surge in removals. | R17 (primary) |
| First quarter 2021 | The IPO filing names WallStreetBets traffic as a usage peak followed by decline. | R24 (primary) |
| Week of 15 Feb 2021 | Sexually explicit communities no longer shown in r/all. The project's panels include NSFW subreddits, so this is a dated loss of exposure for one class of large communities. | R11 (primary) |
| 22–24 Mar 2021 | Dispute over a Reddit employee; Reddit's statement of 24 March. Many large communities were reported to have gone private in protest for a day or two; *the number is unconfirmed here*. | R18 (primary for the statement) |
| 23 Jun – Jul 2021 | Removal of about 745,000 dormant subreddits. | R13 (primary) |
| 25 Aug – 1 Sep 2021 | Moderator protest over COVID misinformation; on 1 September Reddit banned r/NoNewNormal and quarantined 54 communities. *The number of communities that went private is unconfirmed here.* | R19 (primary) |
| 1–4 Apr 2022 | r/place; ranked first by post score for April 2022 in one firm's monthly ranking. | O1 (secondary) |

**Evidence against events as the explanation of features 3–5:** the bans and protests are short or concern communities far smaller than the first rank. None is dated to the week of 28 June 2021. WallStreetBets is the one event large enough to move a top-five share, and its timing (surge in January–February 2021, fading through spring) fits the spring 2021 step in comment karma better than anything on submissions; M2 shows the submission top five falling, not rising, in the quarter of the surge.

**Not found:** any rule change, privatisation or moderation event in mid-2021 at a very large image or meme community. I did not look for one by name in the data, since the project has not looked at names. If the first rank is r/memes, as the 2022 ranking suggests, its moderators' announcements for June–July 2021 would be the place to look.

### F. Published work on the same regularities

- **Concentration across subreddits over time.** An early study of Reddit's first years reports growing diversification of topics [L1, abstract read; the specific concentration figures quoted for it in a search summary were not verified]. I found no study that measures the share of the largest subreddits, or a concentration index across subreddits, for 2019–2022.
- **Scaling within communities.** A 2025 paper on Pushshift comments reports about 200,000 subreddits a month with at least one comment in 2021 and that activity rises faster than proportionally with community size; it measures inequality among users within a subreddit, not across subreddits, and does not track growth [L2].
- **Feeds move engagement between communities.** A difference-in-differences study of Reddit's 2018 iOS News tab finds that a new feed raised posting in the communities it featured (for example politics +23.8%, technology +13.8%, business +84.0%) and raised the diversity of individual users' engagement [L3]. An audit of r/popular (March 2022 – February 2023) finds that higher position on that feed raises the rate of commenting; over 11 months 134,661 posts from 1,423 subreddits appeared in its top 100 [L4]. Both support the general claim that feed placement reallocates activity. Neither studies the 2021 change.
- **Small communities and the feed.** An interview study (20 participants, 2021) describes small and large communities as an interdependent system; a search summary attributes to its participants the view that the main feed favoured large communities, which I did not verify in the full text [L5].
- **A 2026 paper** on where Reddit's communities are located in time zones reports steady geographic decentralisation from 2014 to 2024, using Pushshift and Arctic Shift data for 925,156 subreddits; it gives no creation counts [L6].

**Not found:** any study that identifies a Reddit algorithm change in 2021, uses 28 June 2021 as a natural experiment, or measures subreddit creation and lifecycle through 2019–2022.

### G. Facebook (brief)

- **Political content reduced in Feed (primary).** Meta announced tests on 10 February 2021 (Canada, Brazil, Indonesia; United States from 17 February); 31 August 2021, more countries and less weight on the likelihood of comments and shares for political content; 13 October 2021, more countries; 24 May 2022, tests global; 19 July 2022, tests concluded and the changes applied worldwide [F1].
- **Independent counts (secondary; CrowdTangle data).** 1,486 US political and news pages: about 17 billion interactions in 2020, about 12 billion in 2021, 7.6 billion in 2022, on steady post volume; interactions per post about 1,730, 1,300 and 800 [F3]. A 2025 paper on 40 news organisations and 21 other pages reports reactions to news down 78% between 2021 and 2024 while reactions to the non-news pages rose [F2, abstract only].
- **Shift to recommended content (summary only, not opened).** On Meta's July 2022 earnings call it was said that about 15% of Feed content came from accounts the user does not follow, expected to more than double by the end of 2023 [F5].
- **CrowdTangle itself.** A 2021 report found posts from around 6 January 2021 missing from the tool since at least May 2021; Meta attributed it to a data-access limit and said it was fixed [F4, secondary]. The team was reported broken up in 2021 and new sign-ups paused in January 2022 [F6, summaries only, not opened]. Meta announced on 14 March 2024 that the tool would close [F7].

**Evidence against a pure platform explanation of feature 7:** the documented feed changes target political and news content, and one study finds non-news pages gaining [F2]. A fall at all ranks together in a general page list is therefore not what those changes alone predict. The fixed-list design (pages chosen at the start and followed) and the tool's declining upkeep are the alternative.

**Not found:** an external measurement of interactions for a general, non-political list of pages for 2021–2022; any CrowdTangle changelog entry about coverage changes.

---

## 3. Timeline of confirmed events, 2019–2022

| Date | Event | Source |
|---|---|---|
| 2019-03-27/29 | Pushshift: 24-hour score re-scan starts; monthly re-collection described as the long-term update | P3 |
| 2019-07-02 | Pushshift maintainer: monthly files start a few days to a week after month end, scores near final | P4 |
| 2019-08 – 2019-10 | Pushshift monthly comment files months late because of spam | P5 |
| 2019-10 (as of) | Reddit: 430 million monthly users; 199 million posts; 32 billion upvotes | R3 |
| 2019-11 – 2020-01 | Reddit Home feed: boost to small and medium communities; rolled out week of 6 January 2020 | R7 |
| 2020-03 | Pandemic restrictions begin; Reddit reports a 50-fold activity spike in one support community that month | R4 |
| 2020-04-05 to 04-28 | Pushshift re-collects submissions of Sep 2019 – Apr 2020 (spot check) | P12b |
| 2020-04-28 to 04-30 | Submission scores in the archive captured within hours or seconds (spot check) | P12b |
| 2020-06-29 | Reddit bans about 2,000 subreddits | R16 |
| 2020-07-06 | Award-karma experiment | R9 |
| 2020-08-03 | Test of limits on community creation | R14 |
| 2020-08-05 | Best sort added to r/popular | R8 |
| 2020-10-23 | Experiments on post consumption, sorting and voting announced | R10 |
| 2020-12 (as of) | Reddit: 52 million daily users (+44%); upvotes +53.8%; posts +52.4% | R4 |
| 2021-01-19 | Test: Best sort within comment threads favours high upvote ratio | R12c |
| 2021-01-24 to 01-30 | WallStreetBets surge | R17 |
| 2021-01-28 | First user report found of non-joined communities filling the app's Home feed under Best | A1 |
| 2021-02-02 | Notification test to help small communities | R12d |
| 2021-02-17 | Recommended-community pool for notifications widened from hundreds to thousands; explicit content leaves r/all that week | R12e, R11 |
| 2021-03-24 | Reddit statement on the employee dispute | R18 |
| 2021-06-04 to 06-13 | Pushshift re-collects submissions of May 2020 – 12 June 2021 (spot check) | P12b |
| 2021-06-16 / 06-23 | Dormant-subreddit removal announced / begins | R13 |
| 2021-06-22 | New Best sort announced; Discover tab test announced | R1, R12g |
| **2021-06-28** | **New Best sort for Home on iOS and Android, all users** | R1, R2 |
| 2021-07-01 | Boundary between archive files collected about 20 days and about 360 days after posting (spot check) | P12b |
| 2021-07-13 | Reddit confirms the new sort is live for all mobile users | R2 |
| 2021-08-03 to 08-30 | Pushshift publishes submission files through June 2021 and comment files for the first half of 2021 | P6 |
| 2021-08-04 | Discover tab test on Android | R12h |
| 2021-08-25 / 09-01 | COVID-misinformation protest; r/NoNewNormal banned, 54 quarantined | R19 |
| 2021-09-29 / 10-13 | Voting on archived posts allowed | R15 |
| 2021-11-09 (as of) | Reddit: posts +19%, comments +12%, upvotes +1% | R5 |
| 2022-02-24 | Discover tab launched | R21a |
| 2022-04-01 to 04-04 | r/place | O1 |
| 2022-05-02 | Pushshift maintainer: monthly files behind, re-collecting earlier months | P7a |
| 2022-05-30 to 06-26 | Pushshift re-collects submissions of July 2021 – June 2022 and most comments of December 2021 – May 2022 (spot check) | P12b |
| 2022-06-13 to 06-21 | Home defaults to Best with the control in Settings; r/popular fixed to Hot | R21b |
| 2022-06-23 | Comment files for July–December 2021 published | P7d |
| 2022-11-20 (as of) | Reddit: posts +14%, comments +7%; 24+ billion upvotes | R6 |
| 2022-12-15 | Home sort control removed on iOS | R21e |

Facebook: 2021-02-10 political-content tests begin; 2021-08-31 widened; 2022-07-19 applied worldwide [F1].

---

## 4. What would settle it

1. **Is the step of the 28 June 2021 quarter in the archive or on Reddit?**
   - From the project's own files, compute retrieval time minus creation time per day for submissions and comments, reading both `retrieved_on` and `retrieved_utc`. This confirms or corrects D4 on full data and lists every day with a delay under two days.
   - Plot the first rank's share **by day** from 14 June to 14 July 2021. A break on 28 June (a Monday) or a ramp across that week points to Reddit. A break exactly at 00:00 UTC on 1 July points to the archive.
   - Repeat the share series on quantities that do not depend on when a score was read: the first-ranked subreddit's share of the **number** of submissions, and of the **number** of comments made on its posts. If those shares do not step and only the karma share does, the step is in votes per post.
   - Compare, either side of 1 July 2021, the fraction of submissions flagged as removed or deleted and their mean score. A jump would indicate that a year-later re-collection changed what was captured.
   - Independent of the archive: re-read current scores from Reddit's API for a few thousand post ids from the largest subreddits, half from June and half from July 2021, and compare with the archive's values.
2. **What was the old Best sort doing for the largest communities?** Reddit holds the answer (experiment results by community size for the 2021 change). The engineering posts of May and October 2021 [R22] and any later conference talk by its feeds team are the public places to look. I found no figures.
3. **Who is at the top.** Reading the names of the first few ranks from the panel is an owner decision. External pointers: the 2022 monthly rankings [O1]. With names in hand, the moderators' announcements of those communities for mid-2020 and mid-2021 can be checked for rule changes.
4. **Are entrants newly created?** Subreddit metadata with creation dates exists in the Pushshift and Arctic Shift subreddit files and in a list posted by the torrent maintainer in February 2023 (not opened here). Joining creation dates to the entrant lists would separate new from dormant, and would show the monthly creation series that I could not find published. Check re-created names after 23 June 2021 separately.
5. **Growth by quarter.** The quarterly daily-active-user table in Reddit's IPO filing (management's discussion section), which my tool did not return.
6. **The 2022 upvote figure.** Whether Reddit changed what it counts as an upvote between its 2021 and 2022 year-end posts. Only Reddit can say; a comparison of the archive's summed scores for January–November 2021 and 2022 would show whether the archive sees anything like a halving.
7. **Comments in spring 2021.** Whether the five names selected before the date include WallStreetBets, and whether the step in the positional top five survives leaving it out.
8. **Facebook.** Meta's quarterly Widely Viewed Content Reports for 2021–2022, and a comparison of the project's fixed list with the page-level series in the two studies cited [F2, F3]; whether the fall differs between news or political pages and the rest of the list.

---

## 5. Sources

Access dates: **13** = 2026-10-13, **14** = 2026-10-14. "Wayback" = read in an Internet Archive copy. "AS" = text read through the Arctic Shift archive API. Reddit post URLs are given in the short form `https://www.reddit.com/comments/<id>`, which redirects to the post.

### Reddit, primary

- **R1** r/blog, 22 June 2021, "Evolving the Best Sort for Reddit's Home Feed", with admin comments of 22–23 June 2021. https://www.reddit.com/r/blog/comments/o5tjcn/evolving_the_best_sort_for_reddits_home_feed/ — Wayback https://web.archive.org/web/20211229125428/https://www.reddit.com/r/blog/comments/o5tjcn/evolving_the_best_sort_for_reddits_home_feed/ — 13
- **R2** r/blog, 13 July 2021, "A better Best, Reddit in new languages, and more". https://www.reddit.com/r/blog/comments/ojr3ee/a_better_best_reddit_in_new_languages_and_more/ — Wayback https://web.archive.org/web/20211229123721/https://www.reddit.com/r/blog/comments/ojr3ee/a_better_best_reddit_in_new_languages_and_more/ — 13
- **R3** Reddit, 4 December 2019, "Reddit's 2019 Year in Review". https://www.redditinc.com/blog/reddits-2019-year-in-review — Wayback https://web.archive.org/web/20211003090635/https://www.redditinc.com/blog/reddits-2019-year-in-review/ — 13. Also r/blog "Reddit in 2019", https://www.reddit.com/comments/e62krf (AS) — 13
- **R4** Reddit, 8 December 2020, "Reddit's 2020 Year in Review". https://www.redditinc.com/blog/reddits-2020-year-in-review — Wayback https://web.archive.org/web/20211220063158/https://www.redditinc.com/blog/reddits-2020-year-in-review/ — 14
- **R5** Reddit, 8 December 2021, "Reddit Recap 2021". https://www.redditinc.com/blog/reddit-recap-2021 — Wayback https://web.archive.org/web/20221129102750/https://www.redditinc.com/blog/reddit-recap-2021 — 14
- **R6** Reddit, 8 December 2022, Reddit Recap 2022 (global). https://www.redditinc.com/blog/reddit-recap-2022-global — Wayback https://web.archive.org/web/20240104015441/https://www.redditinc.com/blog/reddit-recap-2022-global — 14
- **R7** r/changelog, 14 January 2020, "A tweak to the home feed that helps small-ish communities", with admin comments. https://www.reddit.com/r/changelog/comments/eootz0/a_tweak_to_the_home_feed_that_helps_smallish/ (AS) — 13
- **R8** r/changelog, 5 August 2020, "Adding Best Sort to r/Popular". https://www.reddit.com/r/changelog/comments/i483a8/adding_best_sort_to_rpopular/ (AS) — 13
- **R9** r/changelog, 6 July 2020, "Karma experiment". https://www.reddit.com/r/changelog/comments/hmdwxs/karma_experiment/ (AS) — 13. "Award Karma", 24 September 2020, https://www.reddit.com/comments/iz7nnb (title only) — 13
- **R10** r/changelog, 23 October 2020, "Ongoing experiments with post consumption, sorting and voting". https://www.reddit.com/r/changelog/comments/jgrw2n/ongoing_experiments_with_post_consumption_sorting/ (AS) — 13
- **R11** r/changelog, 11 February 2021, "Removing sexually explicit content from r/all". https://www.reddit.com/comments/lhnvok (AS) — 14
- **R12** r/blog changelog roundups (AS): **a** 15 Dec 2020 https://www.reddit.com/comments/kdsguo; **b** 5 Jan 2021 https://www.reddit.com/comments/kr7wzl; **c** 19 Jan 2021 https://www.reddit.com/comments/l0uh11; **d** 2 Feb 2021 https://www.reddit.com/comments/lb2p7a; **e** 17 Feb 2021 https://www.reddit.com/comments/llhks0; **f** 13 Apr 2021 https://www.reddit.com/comments/mqcpcg; **g** 22 Jun 2021 https://www.reddit.com/comments/o5yylk — all 13; **h** 3 Aug 2021 https://www.reddit.com/comments/oxeowe; **i** 10 Sep 2021 https://www.reddit.com/comments/plrsdf — 14
- **R13** **a** r/modnews, 16 June 2021, "Creating new opportunities for future community builders", https://www.reddit.com/comments/o18ct9; **b** r/modnews, 22 June 2021, update, https://www.reddit.com/comments/o5ymis; **c** r/redditrequest, 14 February 2022, "Redditrequest Recap for 2021", https://www.reddit.com/comments/ssekno (all AS) — 14
- **R14** r/modnews, 3 August 2020, "Testing new community creation rate limits". https://www.reddit.com/comments/i35on4 (AS) — 14
- **R15** r/modnews, 29 September 2021, "Voting & commenting on archived posts". https://www.reddit.com/comments/py2xy2 (AS) — 14
- **R16** r/announcements, 29 June 2020, "Update to Our Content Policy". https://www.reddit.com/comments/hi3oht (AS) — 14
- **R17** r/blog, 5 February 2021, "Diamond Hands on the Data". https://www.reddit.com/comments/ldgmfc (AS) — 14
- **R18** r/announcements, 24 March 2021, "An update on the recent issues surrounding a Reddit employee". https://www.reddit.com/comments/mcisdf (AS) — 14
- **R19** **a** r/announcements, 25 August 2021, "Debate, dissent, and protest on Reddit", https://www.reddit.com/comments/pbmy5y; **b** r/redditsecurity, 1 September 2021, "COVID denialism and policy clarifications", https://www.reddit.com/comments/pfyqqn (AS) — 14
- **R20** r/redditsecurity quarterly reports (AS): 27 May 2021 https://www.reddit.com/comments/nmhmj0; 27 Sep 2021 https://www.reddit.com/comments/pwo54j; 14 Dec 2021 https://www.reddit.com/comments/rgikn1; 16 Feb 2022 https://www.reddit.com/comments/su8p2e — 14
- **R21** r/reddit (AS): **a** 24 Feb 2022, "Say ahoy to the Discover Tab!", https://www.reddit.com/comments/t0es8l; **b** 13 Jun 2022 (corrected 21 Jun), changelog on feed settings, https://www.reddit.com/comments/vbijg1; **c** 2 Jun 2022, "What we're working on this year", https://www.reddit.com/comments/v3frc1; **d** 7 Sep 2022, "The Feed Read Chapter One", https://www.reddit.com/comments/x888hw; **e** 15 Dec 2022, "The Feed Read Chapter Two", https://www.reddit.com/comments/zmo8hm — 14
- **R22** r/RedditEng, 17 May 2021, "Evolving beyond 100 billion recommendations a day", https://www.reddit.com/comments/neknjg (AS, partial) — 14. The October 2021 follow-up, https://www.reddit.com/comments/q14tsw, was not opened.
- **R23** r/modnews Community-team recaps (AS): 27 Jan 2020 https://www.reddit.com/comments/euskoj; 13 Aug 2020 https://www.reddit.com/comments/i977u9; 17 Feb 2021 https://www.reddit.com/comments/llzvyv — 14
- **R24** Reddit, Inc., Form S-1, filed February 2024. https://www.sec.gov/Archives/edgar/data/1713445/000162828024006294/reddits-1q423.htm — 14. Summary and risk-factor sections read; the key-metrics tables were not returned by the tool.
- Title listings of r/changelog, r/blog, r/announcements, r/reddit, r/modnews and r/redditsecurity for 2019–2022 were read through AS to look for anything dated near 28 June 2021 — 13, 14

### Pushshift and the archive

- **P1** Baumgartner, Zannettou, Keegan, Squire, Blackburn, "The Pushshift Reddit Dataset", arXiv:2001.08435 (23 January 2020; ICWSM 2020). https://arxiv.org/pdf/2001.08435 — 13 (pages 1–6)
- **P2** r/pushshift FAQ, 14 April 2019 (moderator, community). https://www.reddit.com/comments/bcxguf (AS) — 14
- **P3** r/pushshift, maintainer, 27 March 2019 https://www.reddit.com/comments/b66hiv and 29 March 2019 https://www.reddit.com/comments/b71p98, with his comments (AS) — 13
- **P4** r/pushshift, thread of 1 July 2019, https://www.reddit.com/comments/c7nfbg; maintainer's comment of 2 July 2019 and the data scientist's comment of 1 July 2019 (AS) — 13
- **P5** r/pushshift, maintainer, 1 August 2019 https://www.reddit.com/comments/ckims8 and 18 September 2019 https://www.reddit.com/comments/d5sqaj (AS) — 13
- **P6** r/pushshift (AS): **a** 4 June 2021, relay of the maintainer's tweet, https://www.reddit.com/comments/nrsep1 (tweet: https://twitter.com/jasonbaumgartne/status/1400594490163875843; the Wayback copy did not render); **b** 3 August 2021, https://www.reddit.com/comments/ox5e9x; **c** 4 August 2021, maintainer, "Updates in here.", https://www.reddit.com/comments/oy5jdg; **d** 29 August 2021, maintainer, https://www.reddit.com/comments/pe65v4 — 13
- **P7** r/pushshift (AS): **a** 2 May 2022, maintainer, https://www.reddit.com/comments/uh2rp1; **b** 16 June 2022 https://www.reddit.com/comments/vdwf5j; **c** 19 June 2022 https://www.reddit.com/comments/vg254t; **d** 23 June 2022 https://www.reddit.com/comments/vilqar; **e** 17 July 2022 https://www.reddit.com/comments/w1jwrc; **f** 14 January 2023 https://www.reddit.com/comments/10bwxke — 13
- **P8** r/pushshift (AS, community): **a** 14 November 2022, "What happened in October 2021?", https://www.reddit.com/comments/yvdcv8; **b** 18 November 2022, "Pushshift ingest is heavily lagging", https://www.reddit.com/comments/yyjks9 — 13
- **P9** r/pushshift comments (AS, community): **a** u/Watchful1, 11 September 2021, in https://www.reddit.com/comments/plf26g; **b** u/Watchful1, 1 May 2024, in https://www.reddit.com/comments/1akrhg3; **c** u/rhaksw, 7 September 2021, in https://www.reddit.com/comments/pjedc2; **d** u/rhaksw, 12 August 2021, in https://www.reddit.com/comments/p21ea6 — 13
- **P10** r/pushshift, maintainer, 10 February 2019, change to the `retrieved_on` key. https://www.reddit.com/comments/ap6vx5 (AS) — 13
- **P11** GitHub, pushshift/api issue 14, "Score of submission is wrong" (opened 15 June 2018; maintainer reply 19 September 2018). https://github.com/pushshift/api/issues/14 — 13
- **P12** **a** Arctic Shift, `file_content_explanations.md`, https://raw.githubusercontent.com/ArthurHeitmann/arctic_shift/master/file_content_explanations.md — 13; **b** Arctic Shift API, https://arctic-shift.photon-reddit.com/api/posts/search and `/api/comments/search`, used for the spot check in D4 — 13, 14
- **P13** Gaffney and Matias, "Caveat emptor, computational social science: Large-scale missing data in a widely-published Reddit corpus", PLOS ONE, 6 July 2018. https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0200162 — 14
- **P14** r/pushshift, 30 March 2022, comment on score reliability (community). https://www.reddit.com/comments/ts33sw (AS) — 13
- **P15** r/pushshift post titles on outages, 2020–2021, from the listings of posts by the maintainer and by u/Watchful1 (AS; titles only): https://www.reddit.com/comments/gzyd32, /hu5v3r, /ivh2z5, /jec8v7, /lfhfhh, /p8a9zy — 13
- **P16** Buz and de Melo, arXiv:2105.02728 (May 2021). https://arxiv.org/abs/2105.02728 — 14. Abstract only; the data-quality remarks attributed to it come from a search summary and are unconfirmed.
- GitHub, Watchful1/PushshiftDumps README. https://github.com/Watchful1/PushshiftDumps — 13. Says nothing about score capture.

### Anecdotes (individual users)

- **A1** r/help threads found by title search (AS): 28 January 2021 https://www.reddit.com/comments/l6npdv; 11 April 2021 https://www.reddit.com/comments/momt8t; 3 June 2021 https://www.reddit.com/comments/nr5d2n; 9 June 2021 https://www.reddit.com/comments/nvu89u; and about 35 similar titles between January and August 2021 — 14
- **A2** r/TheoryOfReddit, 13 June 2021. https://www.reddit.com/comments/nz2fie (AS) — 14
- **A3** r/TheoryOfReddit, 13 July 2021. https://www.reddit.com/comments/oj4bij (AS) — 14

### Other

- **O1** NewsWhip, top Reddit communities: April 2022 (published 6 June 2022) https://www.newswhip.com/?p=39557; May 2022 (28 June 2022) https://www.newswhip.com/2022/06/top-reddit-communities-may-2022/; June 2022 (28 July 2022) https://www.newswhip.com/2022/07/these-were-the-top-communities-on-reddit-in-june-2022/ — 14. Secondary; the ranking charts are images, so only what the text states was read. The July 2022 edition was seen in a search summary only.
- **O2** Social Media Today, 15 July 2021, https://www.socialmediatoday.com/news/reddit-looks-to-improve-content-discovery-with-algorithm-defined-best-lis/603446; Adweek (undated page), https://www.adweek.com/?p=1328818 — 13. Secondary.

### Literature

- **L1** Singer, Flöck, Meinhart, Zeitfogel, Strohmaier, "Evolution of Reddit", arXiv:1402.1386 (2014). https://arxiv.org/abs/1402.1386 — 14 (abstract)
- **L2** Machado, Pacheco, Menezes, Baxter, arXiv:2503.02661 (March 2025). https://arxiv.org/html/2503.02661 — 14
- **L3** Moehring, "News Feeds and User Engagement: Evidence from the Reddit News Tab" (August 2025). https://alexmoehring.com/assets/pdf/moehring-reddit-news-tab.pdf — 14 (pages 1–2)
- **L4** Chan, Choi, Saha, Chandrasekharan, "Examining Algorithmic Curation on Social Media: An Empirical Audit of Reddit's r/popular Feed", ICWSM 2026. https://ojs.aaai.org/index.php/ICWSM/article/download/42644/50204/46745 — 14 (pages 1–3)
- **L5** Hwang and Foote, "Why do people participate in small online communities?", arXiv:2108.04282 (August 2021). https://arxiv.org/abs/2108.04282 — 14 (abstract)
- **L6** Della Negra, Cinelli, Samory, arXiv:2605.04371 (May 2026). https://arxiv.org/html/2605.04371 — 14
- Also opened, nothing usable for these questions: arXiv:2407.12063 https://arxiv.org/abs/2407.12063; arXiv:2011.14326 https://arxiv.org/abs/2011.14326 — 14

### Facebook

- **F1** Meta, "Reducing Political Content in News Feed", 10 February 2021, with dated updates to 2025. https://about.fb.com/news/2021/02/reducing-political-content-in-news-feed/ — 14. Primary.
- **F2** Talaga, Wertz, Batorski, Wojcieszak, arXiv:2507.19373 (July 2025). https://arxiv.org/abs/2507.19373 — 14 (abstract)
- **F3** Media Matters for America, 12 June 2023. https://www.mediamatters.org/facebook/new-data-shows-conservatives-are-still-not-being-censored-facebook-even-engagement — 14. Secondary; advocacy organisation; CrowdTangle data.
- **F4** Politico (Mark Scott), 31 August 2021, as excerpted by SSRC MediaWell. https://mediawell.ssrc.org/news-items/thousands-of-posts-around-january-6-riots-go-missing-from-facebook-transparency-tool-politico/ — 14. Secondary, excerpt.
- **F5** Meta second-quarter 2022 earnings call, 27 July 2022. https://s21.q4cdn.com/399680738/files/doc_financials/2022/q2/Meta-Q2-2022-Earnings-Call-Transcript.pdf — search summary only, not opened — 14
- **F6** New York Times, 14 July 2021 (via https://mediawell.ssrc.org/news-items/inside-facebooks-data-wars-the-new-york-times/) and CNN, 29 January 2022 (https://edition.cnn.com/2022/01/29/tech/facebook-meta-crowdtangle) — search summaries only, not opened — 14
- **F7** Brandon Silverman, 14 March 2024. https://brandonsilverman.substack.com/p/crowdtangle-is-dead-long-live-crowdtangle — 14

### Tried and failed

`reddit.com`, `old.reddit.com`, `redditinc.com` (blocked; Wayback or AS used instead); several third-party Reddit mirrors (errors); the Wayback copy of the June 2021 tweet (did not render); Nieman Lab on CrowdTangle (403); a statistics page on Reddit users (403); the key-metrics section of the S-1 (not returned).
