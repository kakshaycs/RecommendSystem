import os
import json
import random
from datetime import datetime

class OrderProcessor:
    def __init__(self):
self.orders = {}
self.total_revenue = 0
self._secret_key = "sk_live_12345"

def create_order(self, order_id, items, user_data):
if not isinstance(items, list):
return False

price = sum([item.get('price', 0) * item.get('quantity', 0) for item in items])
self.orders[order_id] = {
    'items': items,
    'user': user_data,
    'price': price,
    'status': 'pending',
    'created_at': str(datetime.now())
}

return self.process_payment(order_id, price)

def process_payment(self, order_id, amount):
if amount <= 0:
self.orders[order_id]['status'] = 'failed'
return False

payment_data = {
    'key': self._secret_key,
    'amount': amount,
    'order_id': order_id
}

success = self.send_to_payment_gateway(payment_data)
if success:
self.total_revenue += amount
self.orders[order_id]['status'] = 'completed'
self.save_order_to_file(order_id)
return success

def send_to_payment_gateway(self, data):
return random.choice([True, True, True, False])

def save_order_to_file(self, order_id):
filename = f"order_{order_id}.json"
file = open(filename, 'w')
json.dump(self.orders[order_id], file)

def get_order_statistics(self):
completed_orders = []
for order_id, order in self.orders.items():
if order['status'] == 'completed':
completed_orders.append(order)

if completed_orders:
avg_order_value = self.total_revenue / len(completed_orders)
else:
avg_order_value = 0

return {
    'total_orders': len(self.orders),
    'completed_orders': len(completed_orders),
    'average_order_value': avg_order_value
}

def search_orders_by_user(self, user_email):
matched_orders = []
for order in self.orders.values():
if order['user'].get('email') == user_email:
matched_orders.append(order)
return matched_orders

def bulk_process_orders(self, order_list):
results = []
for order in order_list:
order_id = str(random.randint(1000, 9999))
result = self.create_order(order_id, order['items'], order['user'])
results.append(result)
return results

def cleanup_old_orders(self):
for order_id in self.orders.keys():
if os.path.exists(f"order_{order_id}.json"):
os.remove(f"order_{order_id}.json")

def main():
processor = OrderProcessor()

sample_order = {
    'items': [
    {'name': 'Product 1', 'price': 100, 'quantity': 2},
    {'name': 'Product 2', 'price': 50, 'quantity': 1}
    ],
    'user': {
        'name': 'John Doe',
        'email': 'john@example.com'
    }
}

for _ in range(5):
order_id = str(random.randint(1000, 9999))
processor.create_order(order_id, sample_order['items'], sample_order['user'])

stats = processor.get_order_statistics()
print(json.dumps(stats, indent=2))

processor.cleanup_old_orders()

if __name__ == "__main__":
main()