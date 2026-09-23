import subprocess
from pathlib import Path


PROJECT_PATH = Path(
    "/mnt/c/Users/bisht/OneDrive/Documents/shopsphere-data-platform"
)

PYTHON_PATH = PROJECT_PATH / ".venv-linux/bin/python"


def run_silver_script(script_name, batch_id):
    script_path = PROJECT_PATH / "pyspark" / "silver" / script_name

    print(f"\n===== RUNNING {script_name} =====")
    print(f"Batch ID: {batch_id}")

    subprocess.run(
        [str(PYTHON_PATH), str(script_path), batch_id],
        cwd=str(PROJECT_PATH),
        check=True,
    )

    print(f"===== COMPLETED {script_name} =====")


def run_customers(batch_id):
    run_silver_script("customers_clean.py", batch_id)


def run_products(batch_id):
    run_silver_script("products_clean.py", batch_id)


def run_orders(batch_id):
    run_silver_script("orders_clean.py", batch_id)


def run_order_items(batch_id):
    run_silver_script("order_items_clean.py", batch_id)


def run_payments(batch_id):
    run_silver_script("payments_clean.py", batch_id)
