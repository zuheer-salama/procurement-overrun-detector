-- Detect MSAs where cumulative latest-version SOW commitments
-- exceed the parent agreement ceiling.
--
-- Compatible with SQLite (uses INSTR / SUBSTR / window functions).

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
    FROM procurement_order
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
FROM master_agreement m
INNER JOIN ranked_orders o
    ON o.msa_id = m.msa_id
WHERE o.version_rank = 1
GROUP BY m.msa_code, m.msa_ceiling_amount
HAVING SUM(o.order_ceiling_amount) > m.msa_ceiling_amount
ORDER BY overage_amount DESC;
