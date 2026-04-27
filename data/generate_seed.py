"""
generate_seed.py
----------------
Generates synthetic data for the MSA / SOW ceiling overrun analysis.

Outputs two CSVs into ./data/:
  - master_agreement.csv     (MSAs with ceilings)
  - procurement_order.csv    (procurement orders of mixed types)

The data is tuned so that:
  - ~200 MSAs with ceilings between $500K and $10M, skewed toward the lower end
  - ~3,000 procurement orders, with SOWs (procurement_type_id = 1) making
    up roughly half — the rest are POs, blanket POs, and change orders
  - SOWs are versioned per the BASE-VERSION convention; ~25% are unversioned,
    ~50% have one amendment, ~25% have two or more amendments
  - Roughly 12% of MSAs end up with cumulative latest-version SOW commitments
    that exceed the MSA ceiling — i.e., the query has something to find

Run:  python generate_seed.py
"""

import csv
import random
from pathlib import Path

from faker import Faker

# ---- Reproducibility ----------------------------------------------------------
SEED = 42
Faker.seed(SEED)
random.seed(SEED)
fake = Faker()

# ---- Configuration ------------------------------------------------------------
NUM_MSAS = 200
OVERAGE_TARGET_RATE = 0.12   # ~12% of MSAs should produce an overage

PROCUREMENT_TYPES = {
    1: ("SOW", "Statement of Work"),
    2: ("PO",  "Purchase Order"),
    3: ("BPO", "Blanket PO"),
    4: ("CO",  "Change Order"),
}

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)


# ---- Generate MSAs ------------------------------------------------------------
msas = []
for i in range(1, NUM_MSAS + 1):
    # Triangular distribution: min $500K, max $10M, mode $2M (skews toward lower end)
    ceiling = round(random.triangular(500_000, 10_000_000, 2_000_000), 2)
    msas.append({
        "msa_id": i,
        "msa_code": f"MSA-{i:05d}",
        "client_name": fake.company(),   # not used by the core query; supports follow-up analyses
        "msa_ceiling_amount": ceiling,
    })


# ---- Generate SOWs (procurement_type_id = 1) ---------------------------------
orders = []
order_id = 1
sow_counter = 0

for msa in msas:
    # Decide whether this MSA will end up in overage
    will_overage = random.random() < OVERAGE_TARGET_RATE
    target_utilization = (
        random.uniform(1.05, 1.50) if will_overage
        else random.uniform(0.30, 0.95)
    )
    target_sow_total = msa["msa_ceiling_amount"] * target_utilization

    # Number of distinct base SOWs for this MSA (mean ~7, range 1-30)
    num_base_sows = max(1, int(random.triangular(1, 30, 7)))
    avg_sow_value = target_sow_total / num_base_sows

    for _ in range(num_base_sows):
        sow_counter += 1
        base_code = f"SOW{sow_counter:05d}"

        # Current (latest-version) value of this SOW, with some variance
        current_value = max(1_000, round(random.gauss(avg_sow_value, avg_sow_value * 0.3), 2))

        # Decide versioning pattern
        r = random.random()
        if r < 0.25:
            # Unversioned (no '-' suffix)
            versions = [(base_code, current_value)]
        elif r < 0.75:
            # One amendment: original + version 1
            original_value = round(current_value * random.uniform(0.5, 0.95), 2)
            versions = [
                (base_code, original_value),
                (f"{base_code}-1", current_value),
            ]
        else:
            # Multiple amendments: 2-5 versions, ramping up to current_value
            num_amendments = random.randint(2, 5)
            versions = []
            for v in range(num_amendments + 1):
                code = base_code if v == 0 else f"{base_code}-{v}"
                if v == num_amendments:
                    value = current_value
                else:
                    # Earlier versions are progressively lower
                    value = round(current_value * (0.4 + 0.5 * v / num_amendments), 2)
                versions.append((code, value))

        for code, value in versions:
            orders.append({
                "order_id": order_id,
                "msa_id": msa["msa_id"],
                "procurement_type_id": 1,
                "order_code": code,
                "order_ceiling_amount": value,
            })
            order_id += 1


# ---- Generate non-SOW procurement orders -------------------------------------
# These exist to exercise the procurement_type_id filter — if someone forgets
# the filter, these will get summed in and the result will visibly differ.
num_sow_orders = len(orders)
num_other_orders = num_sow_orders   # roughly 50/50 split

for _ in range(num_other_orders):
    msa = random.choice(msas)
    proc_type = random.choice([2, 3, 4])
    prefix = PROCUREMENT_TYPES[proc_type][0]
    code = f"{prefix}{order_id:06d}"
    value = round(random.uniform(500, 50_000), 2)
    orders.append({
        "order_id": order_id,
        "msa_id": msa["msa_id"],
        "procurement_type_id": proc_type,
        "order_code": code,
        "order_ceiling_amount": value,
    })
    order_id += 1

random.shuffle(orders)


# ---- Write CSVs --------------------------------------------------------------
msa_path = OUTPUT_DIR / "master_agreement.csv"
order_path = OUTPUT_DIR / "procurement_order.csv"

with open(msa_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["msa_id", "msa_code", "client_name", "msa_ceiling_amount"])
    writer.writeheader()
    writer.writerows(msas)

with open(order_path, "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["order_id", "msa_id", "procurement_type_id", "order_code", "order_ceiling_amount"],
    )
    writer.writeheader()
    writer.writerows(orders)


# ---- Summary -----------------------------------------------------------------
sow_count = sum(1 for o in orders if o["procurement_type_id"] == 1)
print(f"Wrote {len(msas):>5,} MSAs to {msa_path}")
print(f"Wrote {len(orders):>5,} procurement orders to {order_path}")
print(f"  of which {sow_count:>5,} are SOWs (procurement_type_id = 1)")
print(f"  and      {len(orders) - sow_count:>5,} are other instrument types")
