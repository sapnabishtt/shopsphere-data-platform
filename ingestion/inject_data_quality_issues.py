import random
from pathlib import Path

import pandas as pd


RAW_DATA_DIR = Path("data/raw")


def inject_customer_issues():
    file_path = RAW_DATA_DIR / "customers.csv"

    df = pd.read_csv(file_path)

    print("Original customers:", len(df))

    # -------------------------
    # 1. NULL emails
    # -------------------------

    null_indices = random.sample(
        list(df.index),
        100
    )

    df.loc[null_indices, "email"] = None

    # -------------------------
    # 2. Duplicate customers
    # -------------------------

    duplicates = df.sample(
        50,
        random_state=42
    )

    df = pd.concat(
        [df, duplicates],
        ignore_index=True
    )

    # -------------------------
    # 3. Customer updates
    # -------------------------

    update_indices = random.sample(
        list(df.index),
        100
    )

    df.loc[
        update_indices,
        "city"
    ] = "Bangalore"

    df.to_csv(
        file_path,
        index=False
    )

    print("Customers after issues:", len(df))


def inject_order_issues():
    file_path = RAW_DATA_DIR / "orders.csv"

    df = pd.read_csv(file_path)

    print("Original orders:", len(df))

    # -------------------------
    # 1. Duplicate orders
    # -------------------------

    duplicates = df.sample(
        500,
        random_state=42
    )

    df = pd.concat(
        [df, duplicates],
        ignore_index=True
    )

    # -------------------------
    # 2. Invalid amounts
    # -------------------------

    invalid_indices = random.sample(
        list(df.index),
        100
    )

    df.loc[
        invalid_indices,
        "total_amount"
    ] = -100

    # -------------------------
    # 3. NULL customer IDs
    # -------------------------

    null_indices = random.sample(
        list(df.index),
        100
    )

    df.loc[
        null_indices,
        "customer_id"
    ] = None

    # -------------------------
    # 4. Orphan customer IDs
    # -------------------------

    orphan_indices = random.sample(
        list(df.index),
        50
    )

    df.loc[
        orphan_indices,
        "customer_id"
    ] = 99999999

    df.to_csv(
        file_path,
        index=False
    )

    print("Orders after issues:", len(df))


def inject_payment_issues():
    file_path = RAW_DATA_DIR / "payments.csv"

    df = pd.read_csv(file_path)

    print("Original payments:", len(df))

    # -------------------------
    # Duplicate payments
    # -------------------------

    duplicates = df.sample(
        100,
        random_state=42
    )

    df = pd.concat(
        [df, duplicates],
        ignore_index=True
    )

    # -------------------------
    # Invalid payment amounts
    # -------------------------

    invalid_indices = random.sample(
        list(df.index),
        50
    )

    df.loc[
        invalid_indices,
        "payment_amount"
    ] = -500

    df.to_csv(
        file_path,
        index=False
    )

    print("Payments after issues:", len(df))


if __name__ == "__main__":

    random.seed(42)

    print("\nInjecting customer issues...")
    inject_customer_issues()

    print("\nInjecting order issues...")
    inject_order_issues()

    print("\nInjecting payment issues...")
    inject_payment_issues()

    print("\nData quality issues injected successfully!")
