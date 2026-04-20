import json
import random
from datetime import datetime, timedelta

def generate_data(num_records=1000):
    sales_data = []
    inventory_data = []
    store_ids = ['STR-001', 'STR-002', 'STR-003']
    skus = ['SKU-A1', 'SKU-B2', 'SKU-C3', 'SKU-D4']

    base_time = datetime.utcnow() - timedelta(days=7)

    for i in range(num_records):
        # Generate messy nested sales data
        txn_time = (base_time + timedelta(minutes=random.randint(1, 10000))).isoformat()
        
        # Introduce purposeful bad data (nulls, missing fields) for them to clean
        customer_id = f"CUST-{random.randint(100, 999)}" if random.random() > 0.1 else None
        
        items = []
        for _ in range(random.randint(1, 4)):
            items.append({
                "sku": random.choice(skus),
                "qty": random.randint(1, 5),
                "price": round(random.uniform(10.0, 100.0), 2)
            })

        sales_data.append({
            "transaction_id": f"TXN-{10000 + i}",
            "timestamp": txn_time,
            "store_id": random.choice(store_ids),
            "customer_id": customer_id,
            "payload": {
                "items": items,
                "payment_method": random.choice(["CREDIT", "CASH", None])
            }
        })

        # Generate inventory snapshots
        if i % 10 == 0:
            inventory_data.append({
                "snapshot_time": txn_time,
                "store_id": random.choice(store_ids),
                "sku": random.choice(skus),
                "stock_level": random.randint(0, 50),
                "status": random.choice(["IN_STOCK", "LOW_STOCK", "OUT_OF_STOCK", "DAMAGED"])
            })

    with open('sales_raw.jsonl', 'w') as f:
        for record in sales_data:
            f.write(json.dumps(record) + '\n')

    with open('inventory_raw.jsonl', 'w') as f:
        for record in inventory_data:
            f.write(json.dumps(record) + '\n')

if __name__ == "__main__":
    generate_data(5000)
    print("Generated sales_raw.jsonl and inventory_raw.jsonl")
