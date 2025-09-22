import pandas as pd
import numpy as np
import random
import string
from datetime import datetime, timedelta

# Config
N_ROWS = 100
OUTPUT_FILE = "messy_data.csv"
np.random.seed(42)
random.seed(42)

def random_date(start, end):
    """Generate a random datetime between two datetime objects."""
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

def random_string(n=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

def introduce_error(val, error_rate=0.05):
    """Inject random anomalies."""
    if pd.isna(val):  # already NaN
        return val
    if random.random() < error_rate:
        choice = random.choice([
            None,                              # missing
            "",                                # blank
            "??",                              # nonsense
            f"{val},{val}",                    # multiple values in one cell
            random_string(5),                  # random junk
            str(val) + " " + random_string(3), # value + garbage
            -9999,                             # absurd negative
            9999999999                         # absurd large
        ])
        return choice
    return val

# Pre-generate some lookup data
products = ["Laptop", "Shoes", "Phone", "TV", "Shirt", "Headphones"]
categories = ["Electronics", "Apparel", "Home", "Sports"]
regions = ["NA", "APAC", "EMEA", "LATAM"]
payment_methods = ["Credit Card", "UPI", "PayPal", "NetBanking", "COD"]
shipping_modes = ["Standard", "Express", "Same-day"]

# Generate base data
df = pd.DataFrame({
    "order_id": np.arange(1, N_ROWS + 1),
    "order_date": [random_date(datetime(2020,1,1), datetime(2023,12,31)).strftime("%Y-%m-%d")
                   for _ in range(N_ROWS)],
    "customer_id": np.random.randint(1000, 9999, size=N_ROWS),
    "customer_name": [random_string(6) for _ in range(N_ROWS)],
    "email": [f"user{idx}@example.com" for idx in range(N_ROWS)],
    "phone": [f"+91{random.randint(6000000000,9999999999)}" for _ in range(N_ROWS)],
    "gender": np.random.choice(["M", "F", "Other"], size=N_ROWS),
    "dob": [random_date(datetime(1960,1,1), datetime(2005,12,31)).strftime("%Y-%m-%d")
            for _ in range(N_ROWS)],
    "product_id": np.random.randint(100, 999, size=N_ROWS),
    "product_name": np.random.choice(products, size=N_ROWS),
    "category": np.random.choice(categories, size=N_ROWS),
    "brand": np.random.choice(["BrandA", "BrandB", "BrandC", "BrandX"], size=N_ROWS),
    "store_id": np.random.randint(1, 100, size=N_ROWS),
    "region": np.random.choice(regions, size=N_ROWS),
    "payment_method": np.random.choice(payment_methods, size=N_ROWS),
    "transaction_status": np.random.choice(["Success", "Failed", "Pending"], size=N_ROWS),
    "shipping_mode": np.random.choice(shipping_modes, size=N_ROWS),
    "quantity": np.random.randint(1, 5, size=N_ROWS),
    "unit_price": np.round(np.random.uniform(10, 5000, size=N_ROWS), 2),
    "discount": np.round(np.random.uniform(0, 0.5, size=N_ROWS), 2),
})

# Derived columns
df["total_amount"] = np.round(df["quantity"] * df["unit_price"] * (1 - df["discount"]), 2)

# Add filler columns to reach ~50
for i in range(21, 51):
    df[f"col_{i}"] = [random.choice([
        random_string(5), random.randint(1, 9999),
        None, "", "N/A"
    ]) for _ in range(N_ROWS)]

# Inject anomalies into random sample
for col in df.columns:
    df[col] = df[col].apply(lambda x: introduce_error(x, error_rate=0.03))

# Save to CSV
df.to_csv(OUTPUT_FILE, index=False)
print(f"Generated {N_ROWS} rows of messy data in {OUTPUT_FILE}")
