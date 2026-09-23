import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker


fake = Faker("en_IN")

random.seed(42)
Faker.seed(42)


DATA_DIR = "data/raw"

os.makedirs(DATA_DIR, exist_ok=True)


# -------------------------
# Customers
# -------------------------

def generate_customers(count=10000):

    customers = []

    states = [
        "Karnataka",
        "Delhi",
        "Maharashtra",
        "Tamil Nadu",
        "Telangana",
        "Gujarat",
        "West Bengal",
        "Rajasthan",
        "Uttar Pradesh",
        "Kerala"
    ]

    for customer_id in range(1, count + 1):

        signup_date = fake.date_between(
            start_date="-3y",
            end_date="today"
        )

        customers.append({
            "customer_id": customer_id,
            "customer_name": fake.name(),
            "email": fake.email(),
            "city": fake.city(),
            "state": random.choice(states),
            "signup_date": signup_date,
            "updated_at": datetime.now()
        })

    return pd.DataFrame(customers)


# -------------------------
# Products
# -------------------------

def generate_products(count=1000):

    categories = [
        "Electronics",
        "Fashion",
        "Home",
        "Beauty",
        "Sports",
        "Books"
    ]

    products = []

    for product_id in range(1, count + 1):

        products.append({
            "product_id": product_id,
            "product_name": fake.word().title() + " " + fake.word().title(),
            "category": random.choice(categories),
            "price": round(random.uniform(100, 100000), 2),
            "supplier_id": f"SUP{random.randint(1, 100):03d}",
            "updated_at": datetime.now()
        })

    return pd.DataFrame(products)


# -------------------------
# Orders
# -------------------------

def generate_orders(count=100000, customer_count=10000):

    statuses = [
        "COMPLETED",
        "CANCELLED",
        "PENDING",
        "SHIPPED"
    ]

    orders = []

    start_date = datetime.now() - timedelta(days=365)

    for order_id in range(1, count + 1):

        order_date = start_date + timedelta(
            days=random.randint(0, 364)
        )

        orders.append({
            "order_id": order_id,
            "customer_id": random.randint(1, customer_count),
            "order_date": order_date.date(),
            "order_status": random.choice(statuses),
            "total_amount": round(random.uniform(200, 150000), 2),
            "updated_at": order_date
        })

    return pd.DataFrame(orders)


# -------------------------
# Order Items
# -------------------------

def generate_order_items(
    orders_count=100000,
    products_count=1000
):

    order_items = []

    order_item_id = 1

    for order_id in range(1, orders_count + 1):

        number_of_items = random.randint(1, 5)

        for _ in range(number_of_items):

            order_items.append({
                "order_item_id": order_item_id,
                "order_id": order_id,
                "product_id": random.randint(1, products_count),
                "quantity": random.randint(1, 5),
                "unit_price": round(
                    random.uniform(100, 100000),
                    2
                )
            })

            order_item_id += 1

    return pd.DataFrame(order_items)


# -------------------------
# Payments
# -------------------------

def generate_payments(orders_count=100000):

    methods = [
        "UPI",
        "CREDIT_CARD",
        "DEBIT_CARD",
        "NET_BANKING"
    ]

    statuses = [
        "SUCCESS",
        "FAILED",
        "REFUNDED"
    ]

    payments = []

    for payment_id in range(1, orders_count + 1):

        payments.append({
            "payment_id": f"PAY{payment_id:06d}",
            "order_id": payment_id,
            "payment_method": random.choice(methods),
            "payment_status": random.choice(statuses),
            "payment_amount": round(
                random.uniform(200, 150000),
                2
            ),
            "payment_date": datetime.now().date()
        })

    return pd.DataFrame(payments)


# -------------------------
# Main
# -------------------------

if __name__ == "__main__":

    print("Generating customers...")
    customers = generate_customers()

    print("Generating products...")
    products = generate_products()

    print("Generating orders...")
    orders = generate_orders()

    print("Generating order items...")
    order_items = generate_order_items()

    print("Generating payments...")
    payments = generate_payments()

    customers.to_csv(
        f"{DATA_DIR}/customers.csv",
        index=False
    )

    products.to_csv(
        f"{DATA_DIR}/products.csv",
        index=False
    )

    orders.to_csv(
        f"{DATA_DIR}/orders.csv",
        index=False
    )

    order_items.to_csv(
        f"{DATA_DIR}/order_items.csv",
        index=False
    )

    payments.to_csv(
        f"{DATA_DIR}/payments.csv",
        index=False
    )

    print("\nData generation completed!")

    print(f"Customers: {len(customers)}")
    print(f"Products: {len(products)}")
    print(f"Orders: {len(orders)}")
    print(f"Order Items: {len(order_items)}")
    print(f"Payments: {len(payments)}")
