"""
Deterministic 2.0 Scoring Engine
=================================
A five-stage, LLM-free scoring pipeline for job descriptions.

Stages
------
0. JD Quality Triage     — flag stubs and sparse JDs before scoring
1. Title Normalization   — synonym map + RapidFuzz fuzzy fallback
2. Seniority Gate        — required-years extraction + multiplier
3. Section-Aware Parser  — split JD into named segments
4. Tiered Keyword Scorer — anchor vs. baseline keywords × section weight
5. Assembly              — combine all signals into a final score

Intended use: instantiate ``ScoringV2Config`` once at startup (feed from
YAML), build the title index with ``build_title_index()``, then call
``score_job_v2()`` per job.  All operations are pure functions except for
pre-compiled regex patterns stored on the config object.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

try:
    from rapidfuzz import process as _fz_process, fuzz as _fuzz
    _HAS_RAPIDFUZZ = True
except ImportError:  # pragma: no cover
    _HAS_RAPIDFUZZ = False


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class AnchorKeyword:
    """Domain-differentiating keyword.  Match in Requirements section = high value."""
    term: str
    weight: int = 15


@dataclass
class BaselineKeyword:
    """Commodity skill keyword — table stakes, lower signal strength."""
    term: str
    weight: int = 5


@dataclass
class ScoringV2Config:
    """
    All tunable parameters for the V2 engine.  Feed from YAML at startup;
    call ``build_title_index()`` after construction to prepare the fast lookup.

    synonym_map
        Maps every known title variant (lowercased) to a canonical ID string.
        e.g. {"sr. ba": "business_analyst", "technical ba": "business_analyst",
               "senior business analyst": "business_analyst"}

    fuzzy_match_threshold
        RapidFuzz ``token_sort_ratio`` floor (0-100).  88 is tight enough to
        avoid false positives on short titles while still catching typos and
        abbreviation variants like "Sr BA" → "Senior Business Analyst".

    seniority_bands
        Maps band name → (min_years_inclusive, max_years_exclusive).

    seniority_multipliers
        Score multiplier applied to the combined keyword score for each band.
        Set "junior" to 0.5 so a perfectly-keyworded entry-level role can't
        outscore a senior role with the same tools.

    section_weights
        Multiplier applied to keyword scores per JD segment.
        requirements=1.0, about_company=0.2 prevents boilerplate inflation.

    anchor_cap / baseline_cap
        Hard ceiling on points from each keyword tier.

    jd_min_chars
        Minimum cleaned character count; below this the JD is a "stub".

    stub_score_drag / sparse_score_drag
        Multiplicative drag applied to final score for low-quality JDs.
        Keeps them visible but penalised rather than silently dropped.
    """

    # ── Title normalisation ──────────────────────────────────────────────
    synonym_map: Dict[str, str] = field(default_factory=dict)
    fuzzy_match_threshold: int = 88

    # ── Seniority gate ───────────────────────────────────────────────────
    user_years_experience: float = 0.0
    seniority_bands: Dict[str, Tuple[float, float]] = field(default_factory=lambda: {
        "junior": (0.0, 2.0),
        "mid":    (2.0, 5.0),
        "senior": (5.0, 12.0),
        "lead":   (12.0, 99.0),
    })
    seniority_multipliers: Dict[str, float] = field(default_factory=lambda: {
        "junior":  0.50,
        "mid":     0.75,
        "senior":  1.00,
        "lead":    1.00,
        "unknown": 0.85,
    })

    # ── Section weights ──────────────────────────────────────────────────
    section_weights: Dict[str, float] = field(default_factory=lambda: {
        "requirements":  1.00,
        "nice_to_have":  0.60,
        "about_company": 0.20,
        "benefits":      0.10,
        "responsibilities": 0.80,
        "fallback":      0.50,
    })

    # ── Keywords ─────────────────────────────────────────────────────────
    anchor_keywords: List[AnchorKeyword] = field(default_factory=list)
    baseline_keywords: List[BaselineKeyword] = field(default_factory=list)
    negative_keywords: List[Tuple[str, int]] = field(default_factory=list)
    anchor_cap: int = 60
    baseline_cap: int = 30
    negative_cap: int = 45

    # ── JD quality ───────────────────────────────────────────────────────
    jd_min_chars: int = 400
    stub_score_drag: float = 0.85
    sparse_score_drag: float = 0.92


# ---------------------------------------------------------------------------
# Title index helper
# ---------------------------------------------------------------------------

def build_title_index(cfg: ScoringV2Config) -> Dict[str, str]:
    """
    Pre-process the synonym map into a fast O(1) lookup dict.
    Keys are lower-stripped; values are canonical IDs.
    Call once at startup and pass the result to every ``normalize_title()`` call.
    """
    return {k.lower().strip(): v for k, v in cfg.synonym_map.items()}


# ---------------------------------------------------------------------------
# Stage 0 — JD Quality Triage
# ---------------------------------------------------------------------------

@dataclass
class JDQuality:
    is_stub: bool = False
    is_sparse: bool = False
    char_count: int = 0
    flags: List[str] = field(default_factory=list)


_REQUIREMENTS_PROBE = re.compile(
    r"\b(requirements?|qualifications?|must[- ]have"
    r"|what you[''\u2019]ll need|you bring|what we[''\u2019]re looking for"
    r"|minimum qualifications?|basic qualifications?)\b",
    re.IGNORECASE,
)


def triage_jd(description: str, cfg: ScoringV2Config) -> JDQuality:
    """Flag stub and sparse JDs without dropping them."""
    clean = re.sub(r"<[^>]+>", " ", description or "")
    clean = re.sub(r"\s+", " ", clean).strip()
    length = len(clean)

    q = JDQuality(char_count=length)

    if length < cfg.jd_min_chars:
        q.is_stub = True
        q.flags.append(f"jd_stub:len={length}")

    if not _REQUIREMENTS_PROBE.search(clean):
        q.is_sparse = True
        q.flags.append("jd_sparse:no_requirements_section")

    return q


# ---------------------------------------------------------------------------
# Stage 1 — Title Normalisation
# ---------------------------------------------------------------------------

_SENIORITY_PREFIXES = frozenset({
    "senior", "sr", "sr.", "principal", "staff", "lead",
    "head", "director", "vp", "vice", "president",
    "jr", "jr.", "junior", "associate", "entry", "level",
})


def _strip_seniority(phrase: str) -> str:
    """Remove leading seniority tokens so 'Sr. BA' → 'ba'."""
    tokens = phrase.lower().split()
    while tokens and tokens[0].rstrip(".") in _SENIORITY_PREFIXES:
        tokens.pop(0)
    return " ".join(tokens)


def normalize_title(
    raw_title: str,
    title_index: Dict[str, str],
    cfg: ScoringV2Config,
) -> Tuple[Optional[str], str]:
    """
    Returns (canonical_id, method).

    method is one of:
      'exact'           — direct hit in synonym map
      'exact_stripped'  — hit after stripping seniority prefix
      'fuzzy:<score>'   — RapidFuzz token_sort_ratio match
      'unresolved'      — no match above threshold
    """
    if not title_index:
        return None, "unresolved"

    title_l = raw_title.lower().strip()

    # 1. Exact hit
    if title_l in title_index:
        return title_index[title_l], "exact"

    # 2. Seniority-stripped exact hit
    stripped = _strip_seniority(title_l)
    if stripped and stripped in title_index:
        return title_index[stripped], "exact_stripped"

    # 2.5. Qualifier-stripped exact hit — handles "Senior PM, Fund Accounting"
    #       by discarding everything after the first ", " or " - " separator.
    for _sep in (", ", " - ", " – "):
        if _sep in title_l:
            _qual_stripped = title_l.split(_sep, 1)[0].strip()
            if _qual_stripped in title_index:
                return title_index[_qual_stripped], "exact_qualifier_stripped"
            _qs_sen = _strip_seniority(_qual_stripped)
            if _qs_sen and _qs_sen in title_index:
                return title_index[_qs_sen], "exact_qualifier_seniority_stripped"

    # 3. RapidFuzz fallback
    if _HAS_RAPIDFUZZ:
        result = _fz_process.extractOne(
            title_l,
            title_index.keys(),
            scorer=_fuzz.token_sort_ratio,
            score_cutoff=cfg.fuzzy_match_threshold,
        )
        if result:
            matched_key, score, _ = result
            return title_index[matched_key], f"fuzzy:{score}"

    return None, "unresolved"


# ---------------------------------------------------------------------------
# Stage 2 — Seniority Gate
# ---------------------------------------------------------------------------

@dataclass
class SeniorityResult:
    required_years: Optional[float]
    band: str                      # junior | mid | senior | lead | unknown
    multiplier: float
    over_qualified: bool = False
    under_qualified: bool = False
    flags: List[str] = field(default_factory=list)


_EXP_PATTERNS: List[re.Pattern] = [
    re.compile(r"(\d+)\s*\+\s*years?(?:\s+(?:of\s+)?(?:experience|exp))?", re.I),
    re.compile(r"at\s+least\s+(\d+)\s+years?", re.I),
    re.compile(r"minimum\s+(?:of\s+)?(\d+)\s+years?", re.I),
    re.compile(r"(\d+)\s+years?\s+of\s+(?:relevant\s+)?(?:experience|exp)", re.I),
    re.compile(r"(\d+)\s+years?\s+(?:experience|exp)\b", re.I),
    re.compile(r"(\d+)\s+years?\s+(?:working|managing|developing|building)", re.I),
    re.compile(r"(?:require|need|requires|needs)\s+(?:at\s+least\s+)?(\d+)\s+years?", re.I),
]

_SENIOR_TITLE_RE = re.compile(
    r"\b(lead|principal|staff|director|head\s+of|vp|vice\s+president|senior|sr\.?)\b", re.I
)
_JUNIOR_TITLE_RE = re.compile(r"\b(junior|jr\.?|entry[- ]level|associate)\b", re.I)


def _extract_required_years(text: str) -> Optional[float]:
    matches: List[float] = []
    for pat in _EXP_PATTERNS:
        for m in pat.finditer(text):
            try:
                y = float(m.group(1))
                # Guard against spurious matches like "Fortune 500" or "2024"
                if 0 < y <= 30:
                    matches.append(y)
            except (ValueError, IndexError):
                pass
    return max(matches) if matches else None


def evaluate_seniority(
    title: str,
    description: str,
    cfg: ScoringV2Config,
) -> SeniorityResult:
    """
    Determine the seniority band of the role and derive a score multiplier.

    Over-qualification is flagged but does NOT reduce the score (a senior
    candidate can still do a senior role).  Under-qualification applies an
    additional 0.6× drag on top of the band multiplier.
    """
    text = f"{title} {description}"
    required = _extract_required_years(text)

    band = "unknown"
    if required is not None:
        for band_name, (lo, hi) in cfg.seniority_bands.items():
            if lo <= required < hi:
                band = band_name
                break

    # Fall back to title signals if years weren't found
    if band == "unknown":
        if _SENIOR_TITLE_RE.search(title):
            band = "senior"
        elif _JUNIOR_TITLE_RE.search(title):
            band = "junior"

    multiplier = cfg.seniority_multipliers.get(band, 0.85)
    flags: List[str] = []
    user_yoe = cfg.user_years_experience

    over_q = required is not None and user_yoe > 0 and user_yoe > required + 5
    under_q = required is not None and user_yoe > 0 and (required - user_yoe) > 2

    if over_q:
        flags.append(f"seniority:overqualified(req={required},user={user_yoe})")
    if under_q:
        drag = 0.6
        multiplier *= drag
        flags.append(
            f"seniority:underqualified(req={required},user={user_yoe},drag={drag})"
        )

    return SeniorityResult(required, band, multiplier, over_q, under_q, flags)


# ---------------------------------------------------------------------------
# Stage 3 — Section-Aware JD Parser
# ---------------------------------------------------------------------------

# Each entry: (section_name, compiled_header_pattern)
# The header pattern matches lines that introduce that section.
# Order matters: first match wins for overlapping boundaries.
_SECTION_HEADERS: List[Tuple[str, re.Pattern]] = [
    ("requirements", re.compile(
        r"(?m)^[^\S\r\n]*(?:requirements?|qualifications?|must[- ]have"
        r"|what you[''\u2019]ll need|you bring"
        r"|what we[''\u2019]re looking for"
        r"|minimum qualifications?|basic qualifications?"
        r"|required skills)[^\n]*$",
        re.IGNORECASE,
    )),
    ("responsibilities", re.compile(
        r"(?m)^[^\S\r\n]*(?:responsibilities|what you[''\u2019]ll do"
        r"|role overview|your role|the role|key responsibilities"
        r"|duties|day[- ]to[- ]day)[^\n]*$",
        re.IGNORECASE,
    )),
    ("nice_to_have", re.compile(
        r"(?m)^[^\S\r\n]*(?:nice[- ]to[- ]have|bonus|preferred"
        r"|plus(?:es)?|great to have"
        r"|additional qualifications?)[^\n]*$",
        re.IGNORECASE,
    )),
    ("about_company", re.compile(
        r"(?m)^[^\S\r\n]*(?:about us|about the company|who we are"
        r"|our mission|our story|company overview"
        r"|the company|about [a-z]+)[^\n]*$",
        re.IGNORECASE,
    )),
    ("benefits", re.compile(
        r"(?m)^[^\S\r\n]*(?:benefits?|perks?|what we offer"
        r"|compensation(?: and benefits?)?|total rewards"
        r"|we offer)[^\n]*$",
        re.IGNORECASE,
    )),
]


@dataclass
class ParsedJD:
    sections: Dict[str, str]   # section_name → body text
    fallback: str              # text not claimed by any recognised section


def parse_jd_sections(description: str) -> ParsedJD:
    """
    Split a raw job description into named segments using header-line anchors.
    Text that precedes the first recognised header, or that follows an
    unrecognised header, is collected into ``fallback``.
    """
    # Replace block-level HTML tags with newlines so section-header patterns
    # that use [^\n]*$ don't consume the entire document on a single line.
    text = re.sub(
        r"<(?:h[1-6]|p|div|li|ul|ol|br|section|article|header|footer|main|tr|td)"
        r"(?:\s[^>]*)?>",
        "\n",
        description or "",
        flags=re.IGNORECASE,
    )
    # Strip remaining inline tags
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\r\n|\r", "\n", text)
    # Collapse runs of blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)

    # Collect all header match positions: (char_start_of_body, section_name)
    boundaries: List[Tuple[int, str]] = []
    for name, pat in _SECTION_HEADERS:
        for m in pat.finditer(text):
            # Body starts after the header line (m.end())
            boundaries.append((m.end(), name))

    if not boundaries:
        return ParsedJD(sections={}, fallback=text.strip())

    boundaries.sort(key=lambda x: x[0])

    sections: Dict[str, str] = {}
    fallback_chunks: List[str] = []

    # Text before the first header → fallback
    first_body_start = boundaries[0][0]
    preamble = text[:text.rfind("\n", 0, first_body_start) + 1].strip()
    if preamble:
        fallback_chunks.append(preamble)

    for i, (body_start, name) in enumerate(boundaries):
        body_end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(text)
        # Wind body_end back to before the next header's actual header line
        # (boundaries store body_start, so body_end IS the next body_start which
        # is already past the header line — correct as-is)
        chunk = text[body_start:body_end].strip()
        if name in sections:
            sections[name] = sections[name] + " " + chunk
        else:
            sections[name] = chunk

    return ParsedJD(sections=sections, fallback=" ".join(fallback_chunks))


# ---------------------------------------------------------------------------
# Stage 4 — Tiered Keyword Scorer
# ---------------------------------------------------------------------------

@dataclass
class KeywordScoreResult:
    anchor_raw: float
    anchor_score: float       # after cap
    baseline_raw: float
    baseline_score: float     # after cap
    negative_raw: float
    negative_score: float     # after cap
    total: float              # anchor_score + baseline_score - negative_score
    hits: List[Tuple[str, str, float]]   # (term, section, adjusted_pts)
    flags: List[str] = field(default_factory=list)


def _make_pattern(term: str) -> re.Pattern:
    """Build a word-boundary-aware regex for a keyword term."""
    norm = re.sub(r"[-_/]+", " ", term.lower().strip())
    norm = re.sub(r"\s+", " ", norm)
    esc = re.escape(norm).replace(r"\ ", r"[\s\-_\/]+")
    # Allow optional trailing 's' on multi-word phrases (e.g. "data platform" → "data platforms")
    if " " in norm and re.search(r"[a-zA-Z]$", norm) and not norm.endswith("s"):
        esc += "s?"
    if re.match(r"^\w", norm) and re.search(r"\w$", norm):
        return re.compile(rf"\b{esc}\b", re.IGNORECASE)
    return re.compile(rf"(?<!\w){esc}(?!\w)", re.IGNORECASE)


def score_keywords(
    parsed: ParsedJD,
    cfg: ScoringV2Config,
) -> KeywordScoreResult:
    """
    Score anchor and baseline keywords across all JD sections.

    Each hit earns ``keyword.weight × section_weight`` points.
    A keyword only scores once per section (deduped by term+section).
    Anchors and baselines are capped independently before subtacting negatives.
    """
    section_weight = cfg.section_weights
    hits: List[Tuple[str, str, float]] = []
    seen: set = set()  # (term, section_name) — prevent double-counting

    anchor_raw = 0.0
    baseline_raw = 0.0
    negative_raw = 0.0

    # Iterate sections then fallback
    segments: List[Tuple[str, str]] = list(parsed.sections.items())
    segments.append(("fallback", parsed.fallback))

    for section_name, text in segments:
        if not text:
            continue
        w = section_weight.get(section_name, section_weight.get("fallback", 0.5))
        blob = text.lower()

        for kw in cfg.anchor_keywords:
            key = (kw.term, section_name)
            if key in seen:
                continue
            if _make_pattern(kw.term).search(blob):
                pts = kw.weight * w
                anchor_raw += pts
                hits.append((kw.term, section_name, pts))
                seen.add(key)

        for kw in cfg.baseline_keywords:
            key = (kw.term, section_name)
            if key in seen:
                continue
            if _make_pattern(kw.term).search(blob):
                pts = kw.weight * w
                baseline_raw += pts
                hits.append((kw.term, section_name, pts))
                seen.add(key)

        for term, penalty in cfg.negative_keywords:
            key = (term, section_name)
            if key in seen:
                continue
            if _make_pattern(term).search(blob):
                # Apply section weight so "healthcare" in Benefits (~0.10)
                # doesn't hit as hard as "healthcare" in Requirements (1.00).
                pts = penalty * w
                negative_raw += pts
                hits.append((term, section_name, -pts))
                seen.add(key)

    anchor_score = min(anchor_raw, cfg.anchor_cap)
    baseline_score = min(baseline_raw, cfg.baseline_cap)
    negative_score = min(negative_raw, cfg.negative_cap)
    total = anchor_score + baseline_score - negative_score

    flags: List[str] = []
    if anchor_raw == 0:
        flags.append("no_anchor_hits")

    return KeywordScoreResult(
        anchor_raw=anchor_raw,
        anchor_score=anchor_score,
        baseline_raw=baseline_raw,
        baseline_score=baseline_score,
        negative_raw=negative_raw,
        negative_score=negative_score,
        total=total,
        hits=hits,
        flags=flags,
    )


# ---------------------------------------------------------------------------
# Stage 5 — Assembly
# ---------------------------------------------------------------------------

@dataclass
class ScoringV2Result:
    # Normalised title
    canonical_title: Optional[str]
    title_method: str             # exact | exact_stripped | fuzzy:<n> | unresolved

    # Sub-results
    seniority: SeniorityResult
    jd_quality: JDQuality
    keyword: KeywordScoreResult

    # Scores
    title_base_pts: float         # passed in from caller (existing title scorer)
    raw_score: float              # title_base_pts + keyword.total
    final_score: float            # raw_score × seniority.multiplier × quality drags

    # Audit trail
    flags: List[str]

    @property
    def fit_band(self) -> str:
        s = self.final_score
        if s >= 85:
            return "Strong Match"
        if s >= 70:
            return "Good Match"
        if s >= 50:
            return "Fair Match"
        if s >= 35:
            return "Weak Match"
        return "Poor Match"

    def summary(self) -> Dict:
        """Compact dict suitable for logging or dashboard display."""
        return {
            "canonical_title": self.canonical_title,
            "title_method": self.title_method,
            "seniority_band": self.seniority.band,
            "seniority_multiplier": self.seniority.multiplier,
            "required_years": self.seniority.required_years,
            "jd_chars": self.jd_quality.char_count,
            "is_stub": self.jd_quality.is_stub,
            "is_sparse": self.jd_quality.is_sparse,
            "anchor_score": self.keyword.anchor_score,
            "baseline_score": self.keyword.baseline_score,
            "negative_score": self.keyword.negative_score,
            "title_base_pts": self.title_base_pts,
            "raw_score": self.raw_score,
            "final_score": round(self.final_score, 2),
            "fit_band": self.fit_band,
            "flags": self.flags,
            "keyword_hits": [(t, s, round(p, 2)) for t, s, p in self.keyword.hits],
        }


def score_job_v2(
    raw_title: str,
    description: str,
    title_base_pts: float,
    cfg: ScoringV2Config,
    title_index: Dict[str, str],
) -> ScoringV2Result:
    """
    Run all five stages and return a ``ScoringV2Result``.

    Parameters
    ----------
    raw_title:
        The raw job title string (e.g. "Sr. Business Analyst – Aladdin").
    description:
        Full job description HTML or plain text.
    title_base_pts:
        Points already awarded by the existing title-weight scorer (Stage 1 of
        the legacy engine).  Pass 0 if you want purely keyword-driven scoring.
    cfg:
        Pre-built config object (instantiate once at startup).
    title_index:
        Pre-built synonym lookup from ``build_title_index(cfg)``.
    """
    all_flags: List[str] = []

    # Stage 0 — JD quality
    jd_quality = triage_jd(description, cfg)
    all_flags.extend(jd_quality.flags)

    # Stage 1 — Title normalisation
    canonical, method = normalize_title(raw_title, title_index, cfg)
    if canonical is None:
        all_flags.append("title:unresolved")

    # Stage 2 — Seniority gate
    seniority = evaluate_seniority(raw_title, description, cfg)
    all_flags.extend(seniority.flags)

    # Stage 3 — Section parsing
    parsed = parse_jd_sections(description)

    # Stage 4 — Keyword scoring
    keyword = score_keywords(parsed, cfg)
    all_flags.extend(keyword.flags)

    # Stage 5 — Assembly
    raw = title_base_pts + keyword.total
    final = raw * seniority.multiplier

    if jd_quality.is_stub:
        final *= cfg.stub_score_drag
        all_flags.append("score_drag:jd_stub")
    if jd_quality.is_sparse:
        final *= cfg.sparse_score_drag
        all_flags.append("score_drag:jd_sparse")

    return ScoringV2Result(
        canonical_title=canonical,
        title_method=method,
        seniority=seniority,
        jd_quality=jd_quality,
        keyword=keyword,
        title_base_pts=title_base_pts,
        raw_score=raw,
        final_score=final,
        flags=all_flags,
    )


# ── Public helpers (used by engine and UI) ───────────────────────────────────

def build_v2_config_from_prefs(prefs: dict):
    """Build a ScoringV2Config + title_index from the job_search_preferences.yaml structure.

    Returns (cfg, title_index, positive_weights, fast_track_base, fast_track_min)
    where positive_weights maps lowercase canonical title → int weight.
    """
    titles_cfg = prefs.get("titles", {})
    positive_keywords: list = titles_cfg.get("positive_keywords", [])
    synonym_map = {kw.lower().strip(): kw.lower().strip() for kw in positive_keywords}

    # Well-known abbreviations — only added when the canonical is already present.
    _abbrev_map = {
        "pm":             "product manager",
        "sr pm":          "senior product manager",
        "sr. pm":         "senior product manager",
        "product mgr":    "product manager",
        "sr product mgr": "senior product manager",
        "tpm":            "technical product manager",
        "sr tpm":         "senior technical product manager",
        "sr. tpm":        "senior technical product manager",
        "sa":             "solutions architect",
        "sr sa":          "senior solutions architect",
        "bsa":            "business systems analyst",
        "sr bsa":         "senior business systems analyst",
    }
    for _abbrev, _canonical in _abbrev_map.items():
        if _canonical in synonym_map and _abbrev not in synonym_map:
            synonym_map[_abbrev] = _canonical

    raw_weights: dict = titles_cfg.get("positive_weights", {})
    positive_weights = {
        str(k).lower().strip(): int(v)
        for k, v in raw_weights.items()
        if isinstance(v, (int, float))
    }
    fast_track_base = float(titles_cfg.get("fast_track_base_score", 50))
    fast_track_min = int(titles_cfg.get("fast_track_min_weight", 8))

    keywords_cfg = prefs.get("keywords", {})
    body_positive: dict = keywords_cfg.get("body_positive", {})
    body_negative: dict = keywords_cfg.get("body_negative", {})

    _anchor_floor = 8
    anchor_keywords = []
    baseline_keywords = []
    for term, weight in body_positive.items():
        if not isinstance(weight, (int, float)):
            continue
        w = int(weight)
        if w >= _anchor_floor:
            anchor_keywords.append(AnchorKeyword(term=str(term), weight=w))
        else:
            baseline_keywords.append(BaselineKeyword(term=str(term), weight=w))

    negative_keywords = [
        (str(term), int(penalty))
        for term, penalty in body_negative.items()
        if isinstance(penalty, (int, float))
    ]

    user_yoe = float(prefs.get("search", {}).get("experience", {}).get("years", 0))

    matching_cfg = prefs.get("scoring", {}).get("keyword_matching", {})
    anchor_cap  = int(matching_cfg.get("anchor_keyword_cap",   60))
    baseline_cap = int(matching_cfg.get("baseline_keyword_cap", 30))
    negative_cap = int(matching_cfg.get("negative_keyword_cap", 45))

    cfg = ScoringV2Config(
        synonym_map=synonym_map,
        fuzzy_match_threshold=88,
        user_years_experience=user_yoe,
        anchor_keywords=anchor_keywords,
        baseline_keywords=baseline_keywords,
        negative_keywords=negative_keywords,
        anchor_cap=anchor_cap,
        baseline_cap=baseline_cap,
        negative_cap=negative_cap,
    )
    title_index = build_title_index(cfg)
    return cfg, title_index, positive_weights, fast_track_base, fast_track_min


def v2_title_pts(
    role: str,
    title_index: Dict[str, str],
    cfg: ScoringV2Config,
    positive_weights: dict,
    fast_track_base: float,
    fast_track_min: int,
) -> float:
    """Return the title-match bonus points for V2 scoring.

    Resolves the raw role string to a canonical title, looks up its weight,
    and maps that weight to a 0-fast_track_base point range.
    """
    canonical, _ = normalize_title(role, title_index, cfg)
    if not canonical:
        return 0.0
    w = positive_weights.get(canonical, 0)
    if not isinstance(w, (int, float)) or w <= 0:
        return 0.0
    if w >= fast_track_min:
        return float(fast_track_base)
    return float(fast_track_base) * w / fast_track_min
