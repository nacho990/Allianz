"""CSV generator."""
import csv #To Read/Write CSV files
import random #Random Data
from datetime import datetime #Manage date/hours
import os #SO operations


outputfile = os.environ.get('OUTPUT', 'E:/CursoPythonUdemyIntro/Allianz/sales_transactions.csv')
N = int(os.environ.get('N', '5000'))


id_product = [f'P{str(i).zfill(4)}' for i in range(1, 201)] #product id creation filling with 0
id_customer = [f'C{str(i).zfill(6)}' for i in range(1, 1001)] #customer id creation filling with 0

start_date = datetime.now()

os.makedirs(os.path.dirname(outputfile), exist_ok=True)


with open(outputfile, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['transaction_id','customer_id','product_id','quantity','timestamp'])
    for i in range(1, N+1):
        transac = f'TR{5000 + i}'
        customer = random.choice(id_customer)
        product = random.choice(id_product)
        qty = random.randint(1, 5000)
        timestamp = datetime.now()
        writer.writerow([transac, customer, product, qty, timestamp])


print(f'Wrote {N} rows to {outputfile}')