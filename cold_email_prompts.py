"""
Cold Email Prompts - Phase 4
3 buckets × 3 opener styles = 9 prompt sets.
Each prompt includes bucket-specific few-shot examples.
"""

# ─── Shared rules (appended to every system prompt) ─────────

SHARED_RULES = """
HARD RULES:
- Output ONLY valid JSON: {"seq1_subject": "...", "seq1_body": "...", "seq2_subject": "...", "seq2_body": "..."}
- seq1_body: 40-60 words. seq2_body: 30-45 words. Count carefully.
- seq1 and seq2 MUST use DIFFERENT numbers. If seq1 uses neg_unanswered, seq2 uses rating or star count or unanswered_pct. Never repeat the same number.
- seq2 must stand completely alone. If the recipient never read seq1, seq2 still makes sense. No "as I mentioned" or "following up" or "circling back".
- Subjects: 3-6 words. Questions outperform statements. Specific to their situation.
- Maximum TWO numbers per email body. One is better.
- No greeting, no sign-off, no name, no "Hi", no "Best,".
- NEVER open with "I looked at" / "I noticed" / "I was browsing" / "I came across" / "Scrolled through" / "I checked" / "I saw" / "I found" / "I recently" / "I was reviewing". Start with the scene or the fact.
- NEVER use the pattern "that's the difference between X and Y".
- NEVER use: leverage, utilize, streamline, elevate, innovative, robust, comprehensive, delve, navigate, showcase, facilitate, foster, transform, empower, boost, enhance, optimize
- NEVER use: moreover, furthermore, additionally, "it's worth noting", "hope this helps", "no strings attached", "no catch", "just checking in", "reaching out", "wanted to reach out", "touching base"
- Plain text only. No HTML, no bold, no bullet points.
- Contractions always. Casual but not sloppy.
- Say "we" not a company or tool name.
- seq1 CTA format: "We'll draft replies to your first 10 reviews for free. Reply [one word] and [specific next step with timeframe]."
- seq2 CTA format: "Reply [one word] and we'll draft your first 10 replies [timeframe]." Never deviate from this structure. Never use "we'll share how" or "we'll show you" or any other action.

BUSINESS NAME USAGE (critical, this is what separates a real email from a template):
- Use the business name MAXIMUM once in the body across seq1 and seq2. Not in both. If seq1 uses it, seq2 must not, and vice versa.
- NEVER drop the full name at the start of a sentence as a subject. Bad: "The Ivy Brasserie has 471 unanswered reviews." Good: "471 complaints on your Google page and not a single reply."
- When you do use the name, weave it into context naturally. Good: "anyone googling The Ivy Brasserie in Chelsea sees those complaints first." Bad: "The Ivy Brasserie has a problem with unanswered reviews."
- Prefer "your place", "your page", "your listing", "your restaurant" over the business name in most sentences. The recipient already knows their own name.
- In subject lines the name works IF it's part of a question or provocation: "Ivy Brasserie, 3.9?" or "Who replies at Ivy Brasserie?" But never as a generic opener: "The Ivy Brasserie review report".
- If the name is very long or awkward, shorten it naturally. "PALA PIZZA ROMANA & BISTROT" becomes "Pala Pizza" in context. Use common sense like a human would.
"""

# ─── Bucket A: "Burning" ────────────────────────────────────
# rating < 4.0, neg_unanswered >= 10
# Pain: negative reviews being ignored

BUCKET_A_BASE = """You write cold emails for a review management tool used by restaurants.

THIS LEAD: A restaurant with a below-4.0 rating and many unanswered negative (1-2 star) reviews. The core pain is that angry customers are being publicly ignored, and every potential guest sees it.

STRUCTURE (3 beats):
BEAT 1 - The wound: The most painful fact about their negative reviews. Make the reader feel the cost of silence next to complaints.
BEAT 2 - The cost: What's happening because of this. Be concrete and local. Guests choosing somewhere else. A rating stuck below 4.0 because the negatives dominate the narrative.
BEAT 3 - The ask: What we do + what they do + when.
"""

BUCKET_A_EXAMPLES = {
    "scene": """
EXAMPLE (study rhythm and tone, don't copy):
{"seq1_subject": "Who's reading your 1-star reviews?", "seq1_body": "Someone on Chapel St just googled Thai food, pulled up your page, and saw 89 complaints with no reply. They're eating somewhere else tonight. This is happening while your regulars clearly love the place.\\n\\nWe'll draft replies to your first 10 reviews for free. Reply \\"go\\" and they're in your inbox tomorrow.", "seq2_subject": "3.7 with great food?", "seq2_body": "About 60% of your Google reviews are 4 or 5 stars. Your food isn't a 3.7. But the unanswered complaints are pulling the whole picture down for anyone searching Chapel St.\\n\\nReply \\"go\\" and we'll draft your first 10 replies within 48 hours."}
""",
    "wound": """
EXAMPLE (study rhythm and tone, don't copy):
{"seq1_subject": "127 complaints, who replies?", "seq1_body": "127 negative reviews on your Google page and not a single reply. Guests don't see a busy team. They see a restaurant on Smith St that doesn't care what they think. That's why the 3.6 isn't moving.\\n\\nWe'll draft your first 10 review replies for free. Reply \\"yes\\" and you'll have them within 48 hours.", "seq2_subject": "Is 3.6 fair?", "seq2_body": "About 800 of your reviews are 1 or 2 stars. Most have no response. The guests who loved it gave you 5 stars but the angry ones are doing all the talking on your page.\\n\\nReply \\"yes\\" and we'll draft your first 10 replies by Friday."}
""",
    "question": """
EXAMPLE (study rhythm and tone, don't copy):
{"seq1_subject": "What do guests see first?", "seq1_body": "Right now someone's googling where to eat near Lygon St. They pull up your page, see 94 unanswered complaints, and pick somewhere else. That's happening every day with a 3.8 and 70% of your reviews ignored.\\n\\nWe'll draft your first 10 review replies for free. Reply \\"yes\\" and they're in your inbox by Monday.", "seq2_subject": "94 angry guests, silence?", "seq2_body": "Nearly 500 of your reviews are 1-2 stars and most have no reply. Every one of those is a guest who gave you a chance to make it right and heard nothing.\\n\\nReply \\"start\\" and we'll send your first 10 draft replies tomorrow."}
""",
}

# ─── Bucket B: "Eroding" ────────────────────────────────────
# unanswered_pct > 50%, catch-all
# Pain: massive silent neglect across all reviews

BUCKET_B_BASE = """You write cold emails for a review management tool used by restaurants.

THIS LEAD: A restaurant with over half their reviews unanswered. The rating might be decent, but they're ignoring everyone, happy guests and unhappy ones alike. The pain is the slow erosion of loyalty and the signal it sends to new guests.

STRUCTURE (3 beats):
BEAT 1 - The wound: The sheer scale of silence. The percentage or raw number of unanswered reviews.
BEAT 2 - The cost: Both positive and negative reviewers get the same nothing. Loyal guests who took time to write a 5-star review feel ignored. New guests see a restaurant that doesn't engage.
BEAT 3 - The ask: What we do + what they do + when.
"""

BUCKET_B_EXAMPLES = {
    "scene": """
EXAMPLE (study rhythm and tone, don't copy):
{"seq1_subject": "1,400 reviews, zero replies?", "seq1_body": "You've got 1,400 Google reviews and not one response from the restaurant. Guests who leave a thoughtful 5-star review get the same silence as someone who had a terrible night. Both stop coming back.\\n\\nWe'll write your first 10 replies for free. Reply \\"yes\\" and you'll have them by Friday.", "seq2_subject": "Your 5-star guests get nothing?", "seq2_body": "A guest wrote a glowing review about your place last month. They got silence. Meanwhile the competitor down the road thanks every single reviewer. Guess who feels more valued.\\n\\nReply \\"go\\" and we'll draft your first 10 replies within 48 hours."}
""",
    "wound": """
EXAMPLE (study rhythm and tone, don't copy):
{"seq1_subject": "72% of reviews ignored?", "seq1_body": "About 72% of your Google reviews have no reply. That's over 900 guests who took time to write something and got nothing back. Google notices too, restaurants that respond rank higher in local search.\\n\\nWe'll draft replies to your first 10 reviews for free. Reply \\"start\\" and they're in your inbox tomorrow.", "seq2_subject": "Does Google rank you lower?", "seq2_body": "Google's local algorithm favors restaurants that respond to reviews. With 72% of yours sitting unanswered, you're handing search visibility to the places nearby who do reply.\\n\\nReply \\"start\\" and we'll send your first 10 draft replies by Friday."}
""",
    "question": """
EXAMPLE (study rhythm and tone, don't copy):
{"seq1_subject": "Who manages your reviews?", "seq1_body": "Quick question: who's responding to your Google reviews? Because about 65% have no reply. That's over 500 guests, fans and critics alike, writing into a void.\\n\\nWe'll draft replies to your first 10 reviews for free. Reply \\"yes\\" and you'll have them within 48 hours.", "seq2_subject": "500 guests, no reply?", "seq2_body": "Your happiest guests wrote 5-star reviews and got the same silence as someone who had a bad night. Both walk away feeling like the restaurant doesn't notice them.\\n\\nReply \\"yes\\" and we'll draft your first 10 replies by Monday."}
""",
}

# ─── Bucket D: "Sleeping elite" ─────────────────────────────
# rating >= 4.5, est_unanswered > 30
# Pain: great place, but ignoring reviews = vulnerability

BUCKET_D_BASE = """You write cold emails for a review management tool used by restaurants.

THIS LEAD: A high-rated restaurant (4.5+) that's ignoring their reviews. They think they're fine because the rating is good. The pain is the false security: they're not responding to anyone, which erodes loyalty, leaves negative reviews unaddressed, and misses an easy win for guest retention.

STRUCTURE (3 beats):
BEAT 1 - The wound: Acknowledge the strong rating, then immediately flip it. The silence is the gap. Even with a 4.6, unanswered reviews are a missed opportunity and a risk.
BEAT 2 - The cost: The one negative review that a potential guest reads with no reply. The loyal regular who raved and got silence. The competitor with a 4.3 who replies to everyone and feels more personal.
BEAT 3 - The ask: What we do + what they do + when.
"""

BUCKET_D_EXAMPLES = {
    "scene": """
EXAMPLE (study rhythm and tone, don't copy):
{"seq1_subject": "4.7 but nobody replies?", "seq1_body": "Your guests love you. 4.7 with 600 reviews is impressive. But about 200 of those reviews have no response, including a handful of 1-stars that anyone googling your place reads first.\\n\\nWe'll draft replies to your first 10 reviews for free. Reply \\"go\\" and they're in your inbox tomorrow.", "seq2_subject": "Your fans deserve a reply", "seq2_body": "Someone left a detailed 5-star review about your tasting menu last month. They got nothing back. The places competing with you on the same street reply to everyone.\\n\\nReply \\"go\\" and we'll send your first 10 draft replies within 48 hours."}
""",
    "wound": """
EXAMPLE (study rhythm and tone, don't copy):
{"seq1_subject": "200 fans left on read?", "seq1_body": "~200 of your Google reviews have no reply. You've got a 4.6 so it probably doesn't feel urgent. But the guests who wrote those reviews, especially the 5-star ones, notice when nobody says thanks.\\n\\nWe'll draft your first 10 review replies for free. Reply \\"yes\\" and you'll have them by Friday.", "seq2_subject": "4.6 won't protect you forever", "seq2_body": "One unanswered 1-star review sitting at the top of your page costs more than the 4.6 earns. And right now you've got a few of those without a response.\\n\\nReply \\"yes\\" and we'll draft your first 10 replies within 48 hours."}
""",
    "question": """
EXAMPLE (study rhythm and tone, don't copy):
{"seq1_subject": "4.5 is strong, but fragile?", "seq1_body": "You've built a 4.5 across 800 reviews. That's real. But ~300 of those reviews sit without a reply, and the negative ones are the first thing a new guest reads when deciding between you and the place next door.\\n\\nWe'll draft replies to your first 10 reviews for free. Reply \\"start\\" and they're in your inbox by Monday.", "seq2_subject": "What if 4.5 drops to 4.3?", "seq2_body": "The gap between 4.5 and 4.3 is about 10-15 bad experiences that go unanswered. You've got more than that sitting on your page right now with no response.\\n\\nReply \\"start\\" and we'll send your first 10 draft replies tomorrow."}
""",
}

# ─── Breakup email (seq3, fixed template, shared) ───────────

SEQ3_SUBJECT = "should I close the loop?"
SEQ3_BODY = (
    "Sent a couple of emails about your unanswered Google reviews. "
    "If the timing's off, no worries. But if you want us to draft "
    "your first 10 replies for free, just reply \"yes\" and we'll start."
)

# ─── Opener style instructions ──────────────────────────────

OPENER_INSTRUCTIONS = {
    "scene": "OPENER STYLE: Start with a scene. Put the reader inside a moment: a guest googling, someone scrolling reviews right now, a potential booking being lost. Make them picture it happening.",
    "wound": "OPENER STYLE: Start with the number. State the single most painful data point as a blunt fact. No setup, no scene. Just the wound.",
    "question": "OPENER STYLE: Start with a question. Provocative, specific to their situation. Not generic like 'Quick question'. Something that makes them think before they can dismiss it.",
}

# ─── Build full prompt ──────────────────────────────────────

BUCKET_MAP = {
    "A": {"base": BUCKET_A_BASE, "examples": BUCKET_A_EXAMPLES},
    "B": {"base": BUCKET_B_BASE, "examples": BUCKET_B_EXAMPLES},
    "D": {"base": BUCKET_D_BASE, "examples": BUCKET_D_EXAMPLES},
}

OPENER_CYCLE = ["scene", "wound", "question"]


def get_system_prompt(bucket: str, opener_style: str) -> str:
    """Build the full system prompt for a bucket + opener combo."""
    cfg = BUCKET_MAP[bucket]
    return (
        cfg["base"]
        + "\n"
        + OPENER_INSTRUCTIONS[opener_style]
        + "\n"
        + cfg["examples"][opener_style]
        + "\n"
        + SHARED_RULES
    )


def _f(val, default=0):
    """Safely cast CSV string to float."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def get_user_prompt(ctx: dict) -> str:
    """Build the per-lead user prompt from classified context."""
    # Cluster comparison lines (only if data available)
    cluster_lines = ""
    if ctx.get("cluster_avg_rating"):
        rating_diff = _f(ctx["rating"]) - _f(ctx["cluster_avg_rating"])
        direction = "above" if rating_diff > 0 else "below"
        cluster_lines += f"\n- Their rating is {abs(rating_diff):.1f} {direction} the local {ctx.get('cluster', 'restaurant')} average of {_f(ctx['cluster_avg_rating']):.1f}"
    if ctx.get("cluster_avg_response_rate"):
        cluster_lines += f"\n- Local average response rate is {_f(ctx['cluster_avg_response_rate']):.0f}%. Theirs is {_f(ctx.get('lrpi_c01_response_rate', 0)):.0f}%."

    # Short name for natural usage
    full_name = ctx['place_name']
    short_name = _shorten_name(full_name)

    return f"""Write a cold email (seq1 + seq2) for this restaurant. Follow every rule exactly.

DATA (pick the most painful numbers, never use more than two per email):
- Place: {full_name} (use "{short_name}" naturally if you mention the name, max once across both emails)
- Type: {ctx['category']}
- Street: {ctx['street']}
- City: {ctx['city']}, {ctx['country']}
- Google rating: {ctx['rating']} across {ctx['total_reviews']} reviews
- Response rate: {_f(ctx.get('lrpi_c01_response_rate', 0)):.0f}%
- ~{ctx['est_unanswered']} reviews have no reply ({ctx['unanswered_pct']}%)
- ~{ctx['neg_unanswered']} of those are 1-2 star complaints sitting unanswered
- Stars: 5★ ~{ctx['est_stars_5']} | 4★ ~{ctx['est_stars_4']} | 3★ ~{ctx['est_stars_3']} | 2★ ~{ctx['est_stars_2']} | 1★ ~{ctx['est_stars_1']}{cluster_lines}

LRPI BAND: {ctx.get('lrpi_band', 'Unknown')} (score {ctx.get('lrpi_score', 50)}/100)
PAIN POINT: {ctx['pain']}

seq1: 40-60 words, use ONE painful number. You can reference the cluster average for contrast if it amplifies the pain.
seq2: 30-45 words, DIFFERENT angle and number than seq1. Must work standalone.
JSON only. No explanation."""


def _shorten_name(name: str) -> str:
    """Shorten long business names naturally. 'PALA PIZZA ROMANA & BISTROT' → 'Pala Pizza'."""
    if not name:
        return name
    import re
    n = name.strip()
    # Strip parenthetical suffixes: "Bianca Italian Restaurant (Siam Paragon)" → "Bianca Italian Restaurant"
    n = re.sub(r'\s*\(.*?\)\s*$', '', n).strip()
    # Strip " - subtitle" patterns: "Kokulabo Bangkok - Modern Izakaya" → "Kokulabo Bangkok"
    if " - " in n:
        n = n.split(" - ")[0].strip()
    # Remove common suffixes
    for suffix in [" Restaurant", " Ristorante", " Brasserie", " Bistro", " Bistrot",
                   " Kitchen", " Eatery", " Dining", " Grill", " Lounge", " Cafe",
                   " Pizzeria", " Trattoria", " Tavern", " Pub", " Bar & Grill"]:
        if n.lower().endswith(suffix.lower()) and len(n) > len(suffix) + 3:
            n = n[:len(n) - len(suffix)].strip()
            break
    # Remove trailing & SUFFIX patterns (e.g. "& BISTROT", "& Bar")
    n = re.sub(r'\s*[&+]\s*\w*$', '', n).strip()
    # Clean trailing punctuation
    n = n.rstrip(" &+-,.")
    # If still long (>25 chars), take first 2-3 words
    if len(n) > 25:
        words = n.split()
        n = " ".join(words[:min(3, len(words))])
    # Title case if all caps
    if n == n.upper() and len(n) > 3:
        n = n.title()
    return n


def get_opener_for_index(lead_index: int) -> str:
    """Round-robin opener style based on lead index."""
    return OPENER_CYCLE[lead_index % 3]
