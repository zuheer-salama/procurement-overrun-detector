# Detecting $40M in MSA Ceiling Overruns Across Over 130 Active Engagements

> *Schema and data have been anonymized; the analytical approach reflects real work performed in a previous role.*

## TL;DR

Northbeam Solutions, a mid-sized professional services firm, was issuing statements of work (SOWs) against client master service agreements (MSAs) without any system-side check on whether cumulative SOW commitments were exceeding the MSA ceiling. I wrote a SQL query that parsed versioned SOW codes, kept only the latest amendment of each, summed them by parent MSA, and surfaced every contract where commitments had exceeded the ceiling. The output identified **over 130 MSAs with 8 figures in cumulative overages**, drove remediation across all of them, and became part of a recurring monitoring cadence.

## The problem

Northbeam's commercial structure is standard for professional services: a client signs a master service agreement (MSA) with a ceiling — say, $5M over three years — and the firm draws against that ceiling by issuing statements of work (SOWs) for specific scopes. Each SOW carries its own dollar value. Each one can be amended (versioned) over time as scope changes.

The contracts platform stored every SOW, including every amendment. But there was no built-in check comparing **the sum of latest-version SOW values** against **the parent MSA ceiling**. Project managers issued SOWs locally, against their understanding of remaining ceiling, without a portfolio-level view. Over hundreds of active MSAs, this was guaranteed to produce silent overruns — and it did, to the tune of tens of millions.

The cost of leaving it unsolved: revenue recognized against work the firm had no contractual right to bill, awkward client renegotiations after the fact, and exposure that nobody had quantified.

## Approach

The analysis had to do five things in sequence:

1. **Filter to the right procurement instrument type.** The procurement orders table commingles SOWs, regular POs, blanket POs, change orders, and other instrument types. Only SOWs are relevant for MSA ceiling reconciliation.
2. **Parse the SOW code** to separate the base SOW number from its version suffix.
3. **Keep only the latest version** of each base SOW, since earlier amendments are superseded.
4. **Sum the latest-version SOW values** grouped by parent MSA.
5. **Surface only those MSAs where the sum exceeded the ceiling**, sorted by dollar exposure so remediation could prioritize the biggest hits first.

All of this in one query, against the live contracts schema.

## The interesting decisions

**The procurement-type filter is doing silent work.** The orders table holds every kind of procurement instrument the firm issues. Forgetting the `procurement_type_id = 1` filter would mix SOWs with regular POs and blanket POs in the sum — destroying the analysis silently, since nothing about the result would *look* wrong. This is the kind of schema quirk that doesn't surface in any data dictionary; you only learn it from working with the system. Most "wrong analyses" against procurement systems trace back to commingled instrument types getting summed together, so I make a habit of pinning the type filter explicitly even when only one type seems relevant in context.

**Version parsing was the linchpin.** SOW codes follow a `BASE-VERSION` convention (e.g., `SOW12345-3` is the third amendment of base SOW12345). Get this wrong and the entire analysis is wrong. Two specific things had to be right:

- **SOWs with no suffix** are originals that were never amended. The query treats these as version 0 by checking `INSTR(order_code, '-') = 0` and short-circuiting. Without this branch, parsing fails on the unsuffixed records — and a meaningful share of the SOW population had no amendments. Missing the branch would have either dropped them entirely (undercount) or thrown a casting error on the integer cast.

- **Latest-version-only, not all versions summed.** Summing every amendment of every SOW would overstate exposure significantly — a SOW amended twice would be counted three times. I used a window function — `ROW_NUMBER() OVER (PARTITION BY base_order_code ORDER BY version_num DESC)` — to rank versions per base SOW and kept only rank 1. The alternative I considered was `MAX(version_num)` followed by a self-join. Same result, more intermediate rows, harder to read. Window function won.

**The `HAVING` clause is doing real work.** It filters the result set to *only* overages. The operational team didn't need to see compliant MSAs — they needed a remediation queue. Putting this filter in `HAVING` rather than `WHERE` was non-negotiable since the comparison runs against an aggregate.

**Sorting by overage amount descending** matters more than it looks. The contracts team had limited bandwidth; surfacing the worst offenders first meant the highest-exposure MSAs got addressed in week one.

## Outcome

The query identified **over 130 MSAs** with cumulative latest-version SOW commitments exceeding their ceilings, totaling **tens of millions of dollars in overages** across the affected portfolio.

The output was the start of a multi-step remediation workflow:

1. **Initial query** (this one) — produced the list of overrun MSAs, ranked by dollar exposure.
2. **Follow-up query** — joined the 138 MSAs back to buyer and buyer-manager records to pull names and email addresses for each affected contract, producing a routing list for outreach.
3. **Remediation kickoff** — buyers and their managers were contacted with the specific MSAs they owned and the dollar exposure on each, then asked to either justify a ceiling amendment with the client or place holds on further SOWs against MSAs that had run out of room.

Two audiences used the output:

- **Leadership** got a portfolio-level view of contractual exposure they hadn't had before which informed how they talked about pipeline risk and how to prioritize remediation effort.
- **The contracts and procurement team** got an actionable, owner-mapped remediation queue — not just "over 130 contracts have a problem," but "here's the buyer for each one and how to reach them."

The query was promoted from a one-off into a scheduled report so the contracts team could catch new overages as they emerged rather than discovering them in batch.

## What I'd do differently

Two things. First, I'd add a tolerance threshold — a minimum overage amount or percentage — so rounding and timing-of-recognition noise didn't produce false positives the team had to dismiss manually. Second, I'd merge the buyer/manager join into the original query so the output is action-ready in a single step rather than requiring a follow-up pull.

## The code

```sql
WITH parsed_orders AS (
    SELECT
        order_id,
        msa_id,
        order_ceiling_amount,
        order_code,
        CASE
            WHEN INSTR(order_code, '-') = 0
                THEN order_code
            ELSE SUBSTR(order_code, 1, INSTR(order_code, '-') - 1)
        END AS base_order_code,
        CASE
            WHEN INSTR(order_code, '-') = 0
                THEN 0
            ELSE CAST(SUBSTR(order_code, INSTR(order_code, '-') + 1) AS INT)
        END AS version_num
    FROM procurement.contracts.procurement_order
    WHERE procurement_type_id = 1   -- 1 = statement of work
),
ranked_orders AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY base_order_code
               ORDER BY version_num DESC
           ) AS version_rank
    FROM parsed_orders
)
SELECT
    m.msa_code                                          AS msa_code,
    m.msa_ceiling_amount                                AS msa_ceiling,
    SUM(o.order_ceiling_amount)                         AS total_sow_commitments,
    COUNT(o.order_id)                                   AS sow_count,
    SUM(o.order_ceiling_amount) - m.msa_ceiling_amount  AS overage_amount
FROM procurement.contracts.master_agreement m
INNER JOIN ranked_orders o
    ON o.msa_id = m.msa_id
WHERE o.version_rank = 1
GROUP BY m.msa_code, m.msa_ceiling_amount
HAVING SUM(o.order_ceiling_amount) > m.msa_ceiling_amount
ORDER BY overage_amount DESC;
```

## Synthetic data

The repo includes a Faker-based seed script (`data/generate_seed.py`) that generates a synthetic dataset designed to exercise every branch of the query:

- **200 MSAs** with ceilings between $500K and $10M, skewed toward the lower end.
- **~12,000 procurement orders** of mixed types (SOWs, regular POs, blanket POs, change orders), with SOWs making up roughly half. The mixed-type design exercises the `procurement_type_id` filter — forgetting the filter produces visibly wrong output, which is the point.
- **~25% of SOWs unversioned**, ~50% with one amendment, ~25% with two or more amendments — so the version-parsing logic exercises both branches.
- **~12% target overage rate** — calibrated so the query returns a meaningful, non-trivial result set rather than an empty table or a flood.

Running `data/generate_seed.py` followed by `load_and_run.py` reproduces a result set of ~26 MSAs in overage status totaling ~$29M, with the largest single overage around $5.5M.
