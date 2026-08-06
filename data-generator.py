import pandas as pd
import numpy as np
import uuid
import random
import datetime
import os
from faker import Faker
from tqdm import tqdm

# Initialize Faker
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Create output directory
output_dir = "ecommerce_data"
os.makedirs(output_dir, exist_ok=True)

# Set data sizes
NUM_CATEGORIES = 500
NUM_PRODUCTS = 100_000
NUM_CUSTOMERS = 1_000_000
NUM_ORDERS = 5_000_000
NUM_ORDER_ITEMS = 20_000_000

# Generate product categories
def generate_product_categories():
    print("Generating product categories...")
    categories = []
    
    # Main categories
    main_categories = [
        "Electronics", "Clothing", "Home & Kitchen", "Beauty", "Sports & Outdoors",
        "Books", "Toys", "Automotive", "Health", "Grocery", "Pet Supplies",
        "Office Products", "Garden", "Baby", "Furniture", "Industrial", 
        "Music", "Movies", "Art", "Jewelry", "Handmade"
    ]
    
    # Generate subcategories
    for i in range(NUM_CATEGORIES):
        if i < len(main_categories):
            category_name = main_categories[i]
            parent_id = None
        else:
            # Create subcategory
            parent_index = random.randint(0, min(len(main_categories) - 1, i - 1))
            parent_id = categories[parent_index]['category_id']
            parent_name = categories[parent_index]['name']
            category_name = f"{parent_name} - {fake.word().capitalize()}"
        
        category = {
            'category_id': i + 1,
            'name': category_name,
            'description': fake.sentence(),
            'parent_id': parent_id,
            'created_at': fake.date_time_between(start_date='-5y', end_date='-1y').strftime('%Y-%m-%d %H:%M:%S')
        }
        categories.append(category)
    
    return pd.DataFrame(categories)

# Generate products
def generate_products(categories_df):
    print("Generating products...")
    
    products = []
    
    # Get all category IDs
    category_ids = categories_df['category_id'].tolist()
    
    for i in tqdm(range(NUM_PRODUCTS)):
        product_id = i + 1
        
        # Generate a product name based on categories
        category_id = random.choice(category_ids)
        category_name = categories_df[categories_df['category_id'] == category_id]['name'].iloc[0]
        
        # Create the product
        product = {
            'product_id': product_id,
            'name': f"{fake.word().capitalize()} {category_name} {fake.word().capitalize()}",
            'description': fake.paragraph(),
            'price': round(random.uniform(5.99, 499.99), 2),
            'cost': None,  # Will be filled after
            'category_id': category_id,
            'sku': f"SKU-{fake.bothify(text='??###')}",
            'inventory_count': random.randint(0, 500),
            'weight': round(random.uniform(0.1, 20.0), 2),
            'created_at': fake.date_time_between(start_date='-5y', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
            'is_active': random.random() > 0.1  # 90% are active
        }
        
        # Set cost at 40-80% of the price
        product['cost'] = round(product['price'] * random.uniform(0.4, 0.8), 2)
        
        products.append(product)
    
    return pd.DataFrame(products)

# Generate customers
def generate_customers():
    print("Generating customers...")
    
    customers = []
    
    for i in tqdm(range(NUM_CUSTOMERS)):
        # Generate address components
        street = fake.street_address()
        city = fake.city()
        state = fake.state_abbr()
        zip_code = fake.zipcode()
        
        # Create registration date (weighted towards more recent dates)
        days_ago = int(np.random.power(0.5) * 1825)  # Biased towards recent (0-5 years)
        registration_date = (datetime.datetime.now() - datetime.timedelta(days=days_ago)).strftime('%Y-%m-%d %H:%M:%S')
        
        # Create customer
        customer = {
            'customer_id': i + 1,
            'email': fake.unique.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'street_address': street,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'country': 'US',
            'phone': fake.phone_number(),
            'registration_date': registration_date,
            'last_login': fake.date_time_between(start_date=registration_date, end_date='now').strftime('%Y-%m-%d %H:%M:%S')
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)

# Generate orders and order items
def generate_orders_and_items(customers_df, products_df):
    print("Generating orders and order items...")
    
    # Get IDs
    customer_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()
    product_prices = products_df.set_index('product_id')['price'].to_dict()
    
    orders = []
    order_items = []
    
    # Prepare distribution: some customers make more orders than others
    # Follow a Pareto distribution (80/20 rule)
    orders_per_customer = np.random.pareto(1.5, len(customer_ids)) + 1
    orders_per_customer = np.minimum(orders_per_customer, 50)  # Cap at 50 orders per customer
    orders_per_customer = orders_per_customer.astype(int)
    
    # Mapping of customers to their order counts
    customer_order_counts = dict(zip(customer_ids, orders_per_customer))
    
    # Generate orders
    order_id_counter = 1
    order_item_id_counter = 1
    
    for customer_id in tqdm(customer_ids):
        num_orders = customer_order_counts[customer_id]
        
        # Skip some customers (those who registered but never ordered)
        if random.random() < 0.05:  # 5% of customers have no orders
            continue
            
        # Get registration date for this customer
        customer_reg_date = pd.to_datetime(customers_df[customers_df['customer_id'] == customer_id]['registration_date'].iloc[0])
        
        for _ in range(num_orders):
            # Order date between registration and now
            days_since_reg = (datetime.datetime.now() - customer_reg_date).days
            if days_since_reg <= 0:
                continue  # Skip if registration date is in the future (shouldn't happen but just in case)
                
            # Order date (weighted towards more recent dates)
            days_ago = int(np.random.power(0.7) * days_since_reg)
            order_date = (datetime.datetime.now() - datetime.timedelta(days=days_ago)).strftime('%Y-%m-%d %H:%M:%S')
            
            # Shipping and other dates
            processing_days = random.randint(0, 2)
            shipping_days = random.randint(1, 7)
            delivery_days = shipping_days + random.randint(1, 3)
            
            processing_date = (pd.to_datetime(order_date) + datetime.timedelta(days=processing_days)).strftime('%Y-%m-%d %H:%M:%S')
            shipping_date = (pd.to_datetime(processing_date) + datetime.timedelta(days=shipping_days)).strftime('%Y-%m-%d %H:%M:%S')
            delivery_date = (pd.to_datetime(shipping_date) + datetime.timedelta(days=delivery_days)).strftime('%Y-%m-%d %H:%M:%S')
            
            # Order status
            current_date = datetime.datetime.now()
            order_datetime = pd.to_datetime(order_date)
            
            if order_datetime > current_date:
                status = 'Pending'
            elif pd.to_datetime(processing_date) > current_date:
                status = 'Processing'
            elif pd.to_datetime(shipping_date) > current_date:
                status = 'Shipped'
            elif pd.to_datetime(delivery_date) > current_date:
                status = 'In Transit'
            else:
                status = 'Delivered'
            
            # Payment information
            payment_method = random.choice(['Credit Card', 'PayPal', 'Apple Pay', 'Google Pay', 'Gift Card'])
            
            # Create order
            order = {
                'order_id': order_id_counter,
                'customer_id': customer_id,
                'order_date': order_date,
                'status': status,
                'payment_method': payment_method,
                'shipping_address': customers_df[customers_df['customer_id'] == customer_id]['street_address'].iloc[0],
                'shipping_city': customers_df[customers_df['customer_id'] == customer_id]['city'].iloc[0],
                'shipping_state': customers_df[customers_df['customer_id'] == customer_id]['state'].iloc[0],
                'shipping_zip': customers_df[customers_df['customer_id'] == customer_id]['zip_code'].iloc[0],
                'shipping_country': 'US',
                'processing_date': processing_date,
                'shipping_date': shipping_date,
                'delivery_date': delivery_date,
                'total_amount': 0  # Will be calculated based on items
            }
            
            # Add order items (random number between 1 and 5)
            num_items = random.choices([1, 2, 3, 4, 5], weights=[0.5, 0.25, 0.15, 0.07, 0.03])[0]
            order_products = random.sample(product_ids, num_items)
            
            order_total = 0
            
            for product_id in order_products:
                # Item quantity (usually 1, sometimes more)
                quantity = random.choices([1, 2, 3, 4, 5], weights=[0.7, 0.15, 0.08, 0.05, 0.02])[0]
                
                # Price at time of order (slightly different from current price)
                current_price = product_prices[product_id]
                historic_price = round(current_price * random.uniform(0.95, 1.05), 2)
                
                # Discounts (most items have no discount)
                discount_pct = random.choices([0, 5, 10, 15, 20], weights=[0.8, 0.1, 0.05, 0.03, 0.02])[0]
                discount = round((discount_pct / 100) * historic_price * quantity, 2)
                
                # Calculate item total
                item_total = round(historic_price * quantity - discount, 2)
                order_total += item_total
                
                # Create order item
                order_item = {
                    'order_item_id': order_item_id_counter,
                    'order_id': order_id_counter,
                    'product_id': product_id,
                    'quantity': quantity,
                    'price': historic_price,
                    'discount': discount,
                    'total': item_total
                }
                
                order_items.append(order_item)
                order_item_id_counter += 1
            
            # Update order total
            order['total_amount'] = round(order_total, 2)
            orders.append(order)
            order_id_counter += 1
            
            # Limit number of orders and items for testing purposes
            if order_id_counter > NUM_ORDERS:
                break
                
            if order_item_id_counter > NUM_ORDER_ITEMS:
                break
        
        if order_id_counter > NUM_ORDERS:
            break
            
        if order_item_id_counter > NUM_ORDER_ITEMS:
            break
    
    return pd.DataFrame(orders), pd.DataFrame(order_items)

# Create smaller sample datasets
def create_sample_datasets(categories_df, products_df, customers_df, orders_df, order_items_df):
    print("Creating sample datasets...")
    
    # Sample sizes for development (about 0.1% of full size)
    sample_categories = categories_df
    sample_products = products_df.sample(min(1000, len(products_df)))
    sample_customers = customers_df.sample(min(1000, len(customers_df)))
    
    # Get sample order IDs from customers
    sample_customer_ids = sample_customers['customer_id'].tolist()
    sample_orders = orders_df[orders_df['customer_id'].isin(sample_customer_ids)].head(5000)
    
    # Get sample order items
    sample_order_ids = sample_orders['order_id'].tolist()
    sample_order_items = order_items_df[order_items_df['order_id'].isin(sample_order_ids)]
    
    # Save sample datasets
    sample_categories.to_csv(f"{output_dir}/sample_product_categories.csv", index=False)
    sample_products.to_csv(f"{output_dir}/sample_products.csv", index=False)
    sample_customers.to_csv(f"{output_dir}/sample_customers.csv", index=False)
    sample_orders.to_csv(f"{output_dir}/sample_orders.csv", index=False)
    sample_order_items.to_csv(f"{output_dir}/sample_order_items.csv", index=False)

# Main execution function
def generate_all_datasets():
    print("Starting data generation...")
    
    # Generate datasets
    categories_df = generate_product_categories()
    products_df = generate_products(categories_df)
    customers_df = generate_customers()
    orders_df, order_items_df = generate_orders_and_items(customers_df, products_df)
    
    # Save full datasets
    print("Saving full datasets...")
    categories_df.to_csv(f"{output_dir}/product_categories.csv", index=False)
    products_df.to_csv(f"{output_dir}/products.csv", index=False)
    customers_df.to_csv(f"{output_dir}/customers.csv", index=False)
    orders_df.to_csv(f"{output_dir}/orders.csv", index=False)
    order_items_df.to_csv(f"{output_dir}/order_items.csv", index=False)
    
    # Create sample datasets
    create_sample_datasets(categories_df, products_df, customers_df, orders_df, order_items_df)
    
    print("Data generation complete!")
    print(f"Files saved to {output_dir}/")
    
    # Print stats
    print("\nDataset Statistics:")
    print(f"Categories: {len(categories_df)}")
    print(f"Products: {len(products_df)}")
    print(f"Customers: {len(customers_df)}")
    print(f"Orders: {len(orders_df)}")
    print(f"Order Items: {len(order_items_df)}")

if __name__ == "__main__":
    generate_all_datasets()
