import os
import json
import csv
from faker import Faker
import random
import time

# Initialize Faker with dynamic seed
fake = Faker()
random.seed(time.time())  # Use the current time to ensure different data each run

# Directory to store the data files
output_dir = 'project_data/new_data'
os.makedirs(output_dir, exist_ok=True)

# Generate Customer Data (customer_data.json)
def generate_customer_data(num_records=100):
    fieldnames = [
        "customer_id", "first_name", "last_name", "email", "phone_number",
        "address", "city", "state", "zip_code", "registration_date"
    ]
    
    customer_data = []
    with open(os.path.join(output_dir, 'customer_data.csv'), mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for _ in range(num_records):
            customer = {
                "customer_id": fake.uuid4(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "phone_number": fake.phone_number(),
                "address":fake.address().replace("\n", " "),
                "city": fake.city(),
                "state": fake.state(),
                "zip_code": fake.zipcode(),
                "registration_date": fake.date_this_decade().isoformat()
            }
            customer_data.append(customer)
            writer.writerow(customer)
    
    return customer_data

# Generate Product Data (product_data.csv) with realistic product names
def generate_product_data(num_records=50):
    categories = [
        "Electronics", "Clothing", "Food", "Books", "Home Appliances", "Toys", "Sports"
    ]
    product_types = {
        "Electronics": ["Smartphone", "Laptop", "Tablet", "Headphones", "Smartwatch", "TV", "Camera"],
        "Clothing": ["T-shirt", "Jeans", "Jacket", "Sweater", "Shoes", "Hat", "Scarf"],
        "Food": ["Pizza", "Burger", "Pasta", "Soda", "Cake", "Ice Cream", "Snack"],
        "Books": ["Novel", "Comic", "Biography", "Self-Help", "Cookbook", "Science Fiction"],
        "Home Appliances": ["Washing Machine", "Refrigerator", "Microwave", "Blender", "Vacuum Cleaner"],
        "Toys": ["Action Figure", "Doll", "Toy Car", "Building Blocks", "Board Game", "Puzzle"],
        "Sports": ["Soccer Ball", "Basketball", "Baseball Bat", "Tennis Racket", "Yoga Mat"]
    }

    product_data = []
    fieldnames = [
        "product_id", "product_name", "category", "price", "stock_quantity", 
        "supplier_name", "supplier_contact"
    ]
    with open(os.path.join(output_dir, 'product_data.csv'), mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(num_records):
            category = random.choice(categories)
            product_name = f"{fake.company()} {random.choice(product_types[category])}"
            product = {
                "product_id": fake.uuid4(),
                "product_name": product_name,
                "category": category,
                "price": round(fake.random_number(digits=4), 2),
                "stock_quantity": fake.random_int(min=10, max=1000),
                "supplier_name": fake.company(),
                "supplier_contact": fake.phone_number()
            }
            product_data.append(product)
            writer.writerow(product)
    
    return product_data

# Generate Sales Data (sales_data.csv) with relationships to customer and product
def generate_sales_data(customers, products, num_records=200):
    fieldnames = [
        "sale_id", "customer_id", "product_id", "sale_date", "quantity", 
        "total_amount", "payment_method"
    ]
    with open(os.path.join(output_dir, 'sales_data.csv'), mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(num_records):
            customer = random.choice(customers)  # Randomly pick a customer
            product = random.choice(products)    # Randomly pick a product
            quantity = fake.random_int(min=1, max=10)
            total_amount = round(quantity * float(product["price"]), 2)
            sale = {
                "sale_id": fake.uuid4(),
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "sale_date": fake.date_this_year().isoformat(),
                "quantity": quantity,
                "total_amount": total_amount,
                "payment_method": fake.random_element(elements=("Credit Card", "Cash", "Online Payment", "Gift Card"))
            }
            writer.writerow(sale)

# Generate Employee Data (employee_data.csv) for reference
def generate_employee_data(num_records=50):
    fieldnames = [
        "employee_id", "first_name", "last_name", "email", "department", 
        "position", "salary", "hire_date"
    ]
    with open(os.path.join(output_dir, 'employee_data.csv'), mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(num_records):
            writer.writerow({
                "employee_id": fake.uuid4(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "department": fake.job(),
                "position": fake.job(),
                "salary": fake.random_int(min=40000, max=120000),
                "hire_date": fake.date_this_decade().isoformat()
            })

# Generate all data files with relationships
customers = generate_customer_data()
products = generate_product_data()
generate_sales_data(customers, products)
generate_employee_data()

print(f"Data files generated in '{output_dir}' directory with realistic product names.")
