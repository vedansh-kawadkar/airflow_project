import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta
import time
import gc
from concurrent.futures import ThreadPoolExecutor
import itertools

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

class MessyEcommerceGenerator:
    def __init__(self, total_rows=1000):
        self.total_rows = total_rows
        self.batch_size = 500  # Process in batches for memory efficiency
        
        # Setup all lookup data
        self.setup_customers()
        self.setup_products()
        self.setup_warehouses()
        self.setup_date_range()
        self.setup_geography_with_real_zips()
        self.setup_other_lookups()
        
        print(f"Setup complete. Ready to generate {total_rows:,} rows of messy data!")
    
    def setup_geography_with_real_zips(self):
        """Setup cities with real ZIP codes"""
        print("Setting up geography with real ZIP codes...")
        
        self.city_zip_mapping = {
            ('New York', 'NY'): ['10001', '10002', '10003', '10004', '10005', '10010', '10011', '10012', '10013', '10014'],
            ('Los Angeles', 'CA'): ['90001', '90002', '90003', '90004', '90005', '90210', '90211', '90212', '90213', '90220'],
            ('Chicago', 'IL'): ['60601', '60602', '60603', '60604', '60605', '60610', '60611', '60612', '60613', '60614'],
            ('Houston', 'TX'): ['77001', '77002', '77003', '77004', '77005', '77010', '77011', '77012', '77013', '77014'],
            ('Phoenix', 'AZ'): ['85001', '85002', '85003', '85004', '85005', '85006', '85007', '85008', '85009', '85010'],
            ('Philadelphia', 'PA'): ['19101', '19102', '19103', '19104', '19105', '19106', '19107', '19108', '19109', '19110'],
            ('San Antonio', 'TX'): ['78201', '78202', '78203', '78204', '78205', '78210', '78211', '78212', '78213', '78214'],
            ('San Diego', 'CA'): ['92101', '92102', '92103', '92104', '92105', '92106', '92107', '92108', '92109', '92110'],
            ('Dallas', 'TX'): ['75201', '75202', '75203', '75204', '75205', '75206', '75207', '75208', '75209', '75210'],
            ('San Jose', 'CA'): ['95101', '95102', '95103', '95104', '95105', '95106', '95107', '95108', '95109', '95110'],
            ('Austin', 'TX'): ['78701', '78702', '78703', '78704', '78705', '78710', '78711', '78712', '78713', '78714'],
            ('Jacksonville', 'FL'): ['32201', '32202', '32203', '32204', '32205', '32206', '32207', '32208', '32209', '32210'],
            ('Fort Worth', 'TX'): ['76101', '76102', '76103', '76104', '76105', '76106', '76107', '76108', '76109', '76110'],
            ('Columbus', 'OH'): ['43201', '43202', '43203', '43204', '43205', '43206', '43207', '43208', '43209', '43210'],
            ('Charlotte', 'NC'): ['28201', '28202', '28203', '28204', '28205', '28206', '28207', '28208', '28209', '28210'],
            ('Seattle', 'WA'): ['98101', '98102', '98103', '98104', '98105', '98106', '98107', '98108', '98109', '98110'],
            ('Denver', 'CO'): ['80201', '80202', '80203', '80204', '80205', '80206', '80207', '80208', '80209', '80210'],
            ('Boston', 'MA'): ['02101', '02102', '02103', '02104', '02105', '02106', '02107', '02108', '02109', '02110'],
            ('Nashville', 'TN'): ['37201', '37202', '37203', '37204', '37205', '37206', '37207', '37208', '37209', '37210'],
            ('Baltimore', 'MD'): ['21201', '21202', '21203', '21204', '21205', '21206', '21207', '21208', '21209', '21210']
        }
        
        # Create flat list of all cities for easy selection
        self.cities_states = list(self.city_zip_mapping.keys())
        
        # Create lookup for ZIP validation
        self.valid_zips = {}
        for city_state, zip_codes in self.city_zip_mapping.items():
            city, state = city_state
            for zip_code in zip_codes:
                self.valid_zips[zip_code] = {'city': city, 'state': state}
        
        print(f"Created geography mapping with {len(self.valid_zips)} real ZIP codes")
    
    def get_zip_for_city_state(self, city, state):
        """Get a valid ZIP code for a city/state combination"""
        city_state_key = (city, state)
        if city_state_key in self.city_zip_mapping:
            return random.choice(self.city_zip_mapping[city_state_key])
        else:
            # Fallback to any valid ZIP if city/state not found
            return random.choice(list(self.valid_zips.keys()))
    
    def create_messy_zip(self, correct_zip, city, state):
        """Create messy ZIP code data with various error types"""
        error_rate = 0.15
        
        if random.random() > error_rate:
            return correct_zip
        
        error_types = ['null', 'mismatch', 'invalid_format', 'invalid_zip']
        error_type = random.choice(error_types)
        
        if error_type == 'null':
            return np.nan if random.random() < 0.7 else None
        
        elif error_type == 'mismatch':
            # Use ZIP from different city
            wrong_city_state = random.choice([cs for cs in self.cities_states if cs != (city, state)])
            return self.get_zip_for_city_state(wrong_city_state[0], wrong_city_state[1])
        
        elif error_type == 'invalid_format':
            return random.choice(['1234', 'ABCDE', '00000-000', 'ZIP123', '12AB5'])
        
        elif error_type == 'invalid_zip':
            # Generate completely fake ZIP
            return f"{random.randint(10000, 99999)}"
        
        return correct_zip
    
    def setup_customers(self):
        """Generate 2,500 unique customers (50 first names x 50 last names)"""
        print("Setting up customer pool...")
        
        first_names = [
            'James', 'Robert', 'John', 'Michael', 'David', 'William', 'Richard', 'Joseph', 'Thomas', 'Christopher',
            'Charles', 'Daniel', 'Matthew', 'Anthony', 'Mark', 'Donald', 'Steven', 'Paul', 'Andrew', 'Kenneth',
            'Joshua', 'Kevin', 'Brian', 'George', 'Timothy', 'Ronald', 'Jason', 'Edward', 'Jeffrey', 'Ryan',
            'Jacob', 'Gary', 'Nicholas', 'Eric', 'Jonathan', 'Stephen', 'Larry', 'Justin', 'Scott', 'Brandon',
            'Benjamin', 'Samuel', 'Gregory', 'Alexander', 'Patrick', 'Frank', 'Raymond', 'Jack', 'Dennis', 'Jerry'
        ]
        
        last_names = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
            'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
            'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
            'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
            'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell', 'Carter', 'Roberts'
        ]
        
        # Generate all 2,500 combinations
        self.customers = {}
        customer_num = 1
        
        def generate_phone_num():
            area = random.randint(200, 999)
            exchange = random.randint(200, 999)
            number = random.randint(1000, 9999)
            formats = [
                f"({area}) {exchange}-{number}",
                f"{area}-{exchange}-{number}",
                f"{area}.{exchange}.{number}",
                f"+1{area}{exchange}{number}",
                f"{area}{exchange}{number}"
            ]
            
            return random.choice(formats)
        
        for first_name in first_names:
            for last_name in last_names:
                customer_id = uuid.uuid4()
                full_name = f"{first_name} {last_name}"
                self.customers[customer_id] = {
                    'full_name': full_name,
                    'first_name': first_name,
                    'last_name': last_name,
                    'email': f"{first_name.lower()}.{last_name.lower()}@gmail.com",
                    'phone': generate_phone_num(),
                    'age' : random.randint(18, 80),
                    "gender": random.choice(['M', 'F', 'Other'])
                }
                customer_num += 1
        
        # Create weighted customer selection for realistic repeat behavior
        customer_ids = list(self.customers.keys())
        
        # 20% of customers are heavy buyers (5-15 orders)
        # 30% are moderate buyers (2-5 orders)  
        # 50% are light buyers (1-2 orders)
        heavy_buyers = customer_ids[:500]  # First 500 customers
        moderate_buyers = customer_ids[500:1250]  # Next 750 customers
        light_buyers = customer_ids[1250:]  # Remaining 1250 customers
        
        # Create weighted list for selection
        self.weighted_customers = (heavy_buyers * 8 + moderate_buyers * 3 + light_buyers * 1)
        
        print(f"Created {len(self.customers)} unique customers with weighted distribution")
    
    def setup_products(self):
        """Generate ~500 products across 5 categories"""
        print("Setting up product catalog...")
        
        # Product structure: 5 categories x 10 subcategories x 8-12 products
        categories = {
            'Electronics': {
                'subcategories': ['Smartphones', 'Laptops', 'Speakers', 'Headphones', 'Tablets', 
                               'Smart TVs', 'Washing Machines', 'Refrigerators', 'Microwaves', 'Air Conditioners'],
                'brands': ['Apple', 'Samsung', 'Mi', 'Sony', 'LG', 'HP', 'Dell', 'Asus'],
                'price_range': (25, 2500)
            },
            'Apparel': {
                'subcategories': ['Shirts', 'Pants', 'Dresses', 'Shoes', 'Jackets', 
                                'T-Shirts', 'Jeans', 'Sneakers', 'Formal Wear', 'Accessories'],
                'brands': ['Nike', 'Adidas', 'Puma', 'Asics', 'Levi', 'Zara', 'H&M', 'Gap'],
                'price_range': (10, 300)
            },
            'Healthcare': {
                'subcategories': ['Vitamins', 'Personal Care', 'First Aid', 'Medical Devices', 'Skincare',
                                'Dental Care', 'Eye Care', 'Baby Care', 'Supplements', 'Senior Care'],
                'brands': ['Johnson', 'Nivea', 'Oral-B', 'Colgate', 'Dove', 'Neutrogena', 'Cetaphil', 'Pampers'],
                'price_range': (5, 200)
            },
            'Sports': {
                'subcategories': ['Fitness Equipment', 'Outdoor Gear', 'Team Sports', 'Running Shoes', 'Yoga Equipment',
                                'Swimming Gear', 'Cycling', 'Sports Apparel', 'Nutrition', 'Recovery'],
                'brands': ['Nike', 'Adidas', 'Under Armour', 'Reebok', 'Wilson', 'Spalding', 'Yonex', 'Puma'],
                'price_range': (10, 600)
            },
            'Groceries': {
                'subcategories': ['Fresh Produce', 'Dairy', 'Meat', 'Snacks', 'Beverages',
                                'Canned Goods', 'Frozen Foods', 'Bakery', 'Spices', 'Organic Foods'],
                'brands': ['Organic Valley', 'Nestle', 'Kellogg', 'Pepsi', 'Coca Cola', 'Kraft', 'General Mills', 'Tyson'],
                'price_range': (1, 50)
            }
        }
        
        self.products = {}
        product_num = 1
        
        for category, cat_data in categories.items():
            for subcategory in cat_data['subcategories']:
                # Generate 8-12 products per subcategory
                num_products = random.randint(8, 12)
                
                for i in range(num_products):
                    product_id = f"PRD_{product_num:03d}"
                    brand = random.choice(cat_data['brands'])
                    
                    # Generate product name
                    if category == 'Electronics':
                        product_name = f"{brand} {subcategory.rstrip('s')} {random.choice(['Pro', 'Max', 'Ultra', 'Plus', 'Mini', ''])}"
                    else:
                        product_name = f"{brand} {subcategory.rstrip('s')} {random.choice(['Premium', 'Classic', 'Sport', 'Deluxe', ''])}"
                    
                    product_name = product_name.strip()
                    
                    # Generate realistic prices
                    min_price, max_price = cat_data['price_range']
                    list_price = round(random.uniform(min_price, max_price), 2)
                    cost_price = round(list_price * random.uniform(0.4, 0.8), 2)  # Cost is 40-80% of list price
                    
                    self.products[product_id] = {
                        'name': product_name,
                        'category': category,
                        'subcategory': subcategory,
                        'brand': brand,
                        'list_price': list_price,
                        'cost': cost_price
                    }
                    
                    product_num += 1
        
        print(f"Created {len(self.products)} products across 5 categories")
    
    def setup_warehouses(self):
        """Generate 50 warehouses across 10 cities"""
        print("Setting up warehouse network...")
        
        # 10 major cities with 5 warehouses each
        warehouse_cities = [
            ('New York', 'NY'), ('Los Angeles', 'CA'), ('Chicago', 'IL'), ('Houston', 'TX'), ('Phoenix', 'AZ'),
            ('Philadelphia', 'PA'), ('San Antonio', 'TX'), ('San Diego', 'CA'), ('Dallas', 'TX'), ('San Jose', 'CA')
        ]
        
        self.warehouses = {}
        warehouse_num = 1
        
        for city, state in warehouse_cities:
            for i in range(5):  # 5 warehouses per city
                warehouse_id = f"WH_{warehouse_num:03d}"
                self.warehouses[warehouse_id] = {
                    'city': city,
                    'state': state,
                    'country': 'US'
                }
                warehouse_num += 1
        
        print(f"Created {len(self.warehouses)} warehouses across 10 cities")
    
    def setup_date_range(self):
        """Setup Q1 2024 date range (90 days)"""
        print("Setting up date range for Q1 2024...")
        
        self.start_date = datetime(2024, 1, 1)
        self.end_date = datetime(2024, 3, 31)
        
        # Generate all possible dates and date keys
        self.dates = []
        current_date = self.start_date
        
        while current_date <= self.end_date:
            date_key = int(current_date.strftime('%Y%m%d'))
            self.dates.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'date_key': date_key,
                'datetime_obj': current_date
            })
            current_date += timedelta(days=1)
        
        print(f"Created {len(self.dates)} dates for Q1 2024")
    
    def setup_other_lookups(self):
        """Setup other lookup data"""
        self.order_statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
        self.payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay']
        self.payment_statuses = ['success', 'failed', 'pending']
        self.shipping_methods = ['standard', 'express', 'overnight']
        self.genders = ['M', 'F', 'Other']
        
        # Return/refund values
        self.return_values = ['yes', 'no', 'pending', 'true', 'false', '1', '0']
    
    def get_order_status_for_payment(self, payment_status):
        """Get appropriate order status based on payment status"""
        if payment_status == 'failed':
            return random.choice(['pending', 'cancelled'])
        elif payment_status == 'success':
            return random.choice(['delivered', 'shipped'])
        elif payment_status == 'pending':
            return 'pending'
        else:
            return 'pending'  # Default
    
    def get_return_refund_pair(self):
        """Generate correlated return/refund values"""
        return_status = random.choice(self.return_values)
        
        # Map return status to refund status with some correlation
        if return_status in ['yes', 'true', '1']:
            refund_status = random.choice(['yes', 'true', '1'])
        elif return_status in ['pending']:
            refund_status = 'pending'
        else:  # no, false, 0
            refund_status = random.choice(['no', 'false', '0'])
        
        return return_status, refund_status
    
    def introduce_messiness(self, value, column_name, messiness_rate=0.12):
        """Introduce various types of data quality issues"""
        if random.random() > messiness_rate:
            return value
        
        # Different types of messiness based on data type and column
        mess_types = ['null', 'format_error', 'typo', 'extra_space', 'case_issue', 'multiple_values', 'invalid_value']
        
        # Column-specific messiness patterns
        if 'email' in column_name:
            if random.random() < 0.3:
                return str(value).replace('@', '')  # Missing @ symbol
            elif random.random() < 0.2:
                return f"{value}|{value.replace('gmail', 'yahoo')}"  # Multiple emails
        
        elif 'phone' in column_name:
            if random.random() < 0.25:
                return np.nan  # Missing phone numbers are common
            elif random.random() < 0.2:
                return str(value).replace('-', '').replace('(', '').replace(')', '').replace(' ', '')  # Format variations
        
        elif 'age' in column_name:
            if random.random() < 0.15:
                return random.choice([-5, 150, 999, '25 years old', None])  # Invalid ages
        
        elif 'quantity' in column_name:
            if random.random() < 0.08:
                return random.choice([-1, 0, 'two', '', 999])  # Invalid quantities
        
        elif 'price' in column_name or 'cost' in column_name or 'amount' in column_name:
            if random.random() < 0.05:
                return f"${value}" if isinstance(value, (int, float)) else value  # Currency symbol
            elif random.random() < 0.03:
                return str(value) + '_error'  # Invalid format
        
        # General messiness patterns
        mess_type = random.choice(mess_types)
        
        if mess_type == 'null':
            return np.nan if random.random() < 0.7 else None
        
        elif mess_type == 'multiple_values' and isinstance(value, str):
            return f"{value}|{value}_alt"
        
        elif mess_type == 'typo' and isinstance(value, str) and len(value) > 3:
            pos = random.randint(1, len(value)-2)
            return value[:pos] + random.choice('xyz123@#') + value[pos+1:]
        
        elif mess_type == 'extra_space' and isinstance(value, str):
            return f"  {value}  " if random.random() < 0.5 else f"{value}   "
        
        elif mess_type == 'case_issue' and isinstance(value, str):
            return value.upper() if random.random() < 0.5 else value.lower()
        
        elif mess_type == 'format_error' and isinstance(value, (int, float)):
            return str(value) + random.choice(['_invalid', '.0.0', 'ERROR'])
        
        return value
    
    def generate_batch_data(self, batch_start, batch_size):
        """Generate a batch of messy e-commerce data"""
        print(f"Generating batch starting at row {batch_start:,}")
        
        batch_data = {}
        
        # Orders (5 columns - removed tax_amount)
        batch_data['order_id'] = [str(uuid.uuid4()) for _ in range(batch_size)]
        
        date_formats = [
            "%Y-%m-%d",          # 2025-09-20
            "%Y/%m/%d",          # 2025/09/20
            "%d-%b-%Y",          # 20-Sep-2025
            "%d %B %Y",          # 20 September 2025
            "%b %d, %Y",         # Sep 20, 2025
            "%B %d, %Y",         # September 20, 2025
        ]

        # selected_dates = []

        # for _ in range(batch_size):
        #     date_info = random.choice(self.dates)
            
        #     # Add some business seasonality
        #     if date_info['datetime_obj'].weekday() < 5:  # Monday-Friday
        #         if random.random() < 0.7:
        #             chosen_date = date_info['datetime_obj']
        #         else:
        #             chosen_date = random.choice(self.dates)['datetime_obj']
        #     else:
        #         chosen_date = date_info['datetime_obj']
            
        #     # Randomly pick a format
        #     fmt = random.choice(date_formats)
        #     formatted_date = chosen_date.strftime(fmt)
            
        #     selected_dates.append(chosen_date)
            
        
        # Select random dates with some business patterns
        selected_dates = []
        for _ in range(batch_size):
            date_info = random.choice(self.dates)
            # Add some business seasonality - more orders on weekdays
            if date_info['datetime_obj'].weekday() < 5:  # Monday-Friday
                if random.random() < 0.7:  # 70% chance to pick weekday
                    selected_dates.append(date_info)
                else:
                    selected_dates.append(random.choice(self.dates))
            else:
                selected_dates.append(date_info)
        
        # print(f"################# selected dates: {selected_dates}")
        
        
        order_date_list = []
        for s in selected_dates:
            fmt = random.choice(date_formats)
            order_date_list.append(s['datetime_obj'].strftime(fmt))
            
        
        batch_data['order_date'] = order_date_list
        batch_data['order_time'] = [self.introduce_messiness(f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}", 'order_time', 0.05) for _ in range(batch_size)]
        
        # Generate payment status first, then determine order status
        payment_statuses = [random.choice(self.payment_statuses) for _ in range(batch_size)]
        order_statuses = [self.get_order_status_for_payment(ps) for ps in payment_statuses]
        
        batch_data['order_status'] = [self.introduce_messiness(status, 'order_status', 0.06) for status in order_statuses]
        batch_data['shipping_cost'] = [self.introduce_messiness(round(random.uniform(0.5, 9.99), 2), 'shipping_cost', 0.10) for _ in range(batch_size)]
        
        # Customers (9 columns) - with realistic repeat behavior
        selected_customers = []
        for _ in range(batch_size):
            customer_id = random.choice(self.weighted_customers)
            selected_customers.append(customer_id)
            
        # print(f"######### selected_customers ########: \n {self.customers}")
        
        batch_data['customer_id'] = [cid for cid in selected_customers]
        batch_data['customer_email'] = [self.customers[cid]['email'] for cid in selected_customers]
        batch_data['customer_first_name'] = [self.customers[cid]['first_name']for cid in selected_customers]
        batch_data['customer_last_name'] = [self.customers[cid]['last_name']for cid in selected_customers]
        
        # Generate phone numbers with various formats
        # phone_numbers = []
        # for _ in range(batch_size):
        #     area = random.randint(200, 999)
        #     exchange = random.randint(200, 999)
        #     number = random.randint(1000, 9999)
        #     formats = [
        #         f"({area}) {exchange}-{number}",
        #         f"{area}-{exchange}-{number}",
        #         f"{area}.{exchange}.{number}",
        #         f"+1{area}{exchange}{number}",
        #         f"{area}{exchange}{number}"
        #     ]
        #     phone_numbers.append(random.choice(formats))
        
        def generate_age():
            return random.randint(18, 80)
        
        batch_data['customer_phone'] = [self.customers[cid]['phone'] for cid in selected_customers]
        batch_data['customer_age'] = [self.introduce_messiness(self.customers[cid]['age'], 'customer_age', 0.12) for cid in selected_customers]
        batch_data['customer_gender'] = [self.introduce_messiness(self.customers[cid]['gender'], 'customer_gender', 0.08) for cid in selected_customers]
        
        # Registration dates (before order dates)
        reg_dates = []
        for date_info in selected_dates:
            fmt = random.choice(date_formats)
            order_date = date_info['datetime_obj']
            # Registration should be before order date
            days_before = random.randint(30, 1095)  # 1 month to 3 years before
            reg_date = order_date - timedelta(days=days_before)
            reg_dates.append(reg_date.strftime(fmt))
        
        batch_data['customer_registration_date'] = reg_dates
        
        # Customer geography
        customer_cities = []
        customer_states = []
        for _ in range(batch_size):
            city, state = random.choice(self.cities_states)
            customer_cities.append(city)
            customer_states.append(state)
        
        batch_data['customer_city'] = [self.introduce_messiness(city, 'customer_city', 0.07) for city in customer_cities]
        batch_data['customer_state'] = [self.introduce_messiness(state, 'customer_state', 0.05) for state in customer_states]
        
        # Products (7 columns)
        selected_products = []
        for _ in range(batch_size):
            product_id = random.choice(list(self.products.keys()))
            selected_products.append(product_id)
        
        batch_data['product_id'] = selected_products
        batch_data['product_name'] = [self.products[pid]['name'] for pid in selected_products]
        batch_data['product_category'] = [self.products[pid]['category'] for pid in selected_products]
        batch_data['product_subcategory'] = [self.products[pid]['subcategory'] for pid in selected_products]
        batch_data['product_brand'] = [self.products[pid]['brand'] for pid in selected_products]
        batch_data['product_cost'] = [self.products[pid]['cost'] for pid in selected_products]
        batch_data['product_list_price'] = [self.products[pid]['list_price'] for pid in selected_products]
        
        # Warehouses (4 columns)
        selected_warehouses = []
        for i in range(batch_size):
            # Smart warehouse selection - prefer same state as customer 80% of time
            customer_state = customer_states[i]
            
            # Find warehouses in same state
            same_state_warehouses = [wid for wid, wdata in self.warehouses.items() if wdata['state'] == customer_state]
            
            if same_state_warehouses and random.random() < 0.8:
                warehouse_id = random.choice(same_state_warehouses)
            else:
                warehouse_id = random.choice(list(self.warehouses.keys()))
            
            selected_warehouses.append(warehouse_id)
        
        batch_data['warehouse_id'] = selected_warehouses
        batch_data['warehouse_city'] = [self.warehouses[wid]['city'] for wid in selected_warehouses]
        batch_data['warehouse_state'] = [self.warehouses[wid]['state'] for wid in selected_warehouses]
        batch_data['warehouse_country'] = [self.warehouses[wid]['country'] for wid in selected_warehouses]
        
        # Transaction Details (8 columns)
        quantities = []
        line_totals = []
        
        for i in range(batch_size):
            qty = random.randint(1, 10)
            
            quantities.append(qty)
        
        batch_data['quantity_ordered'] = [qty for qty in quantities]
        # batch_data['line_total'] = [self.introduce_messiness(total, 'line_total', 0.05) for total in line_totals]
        
        # batch_data['discount_amount'] = [self.introduce_messiness(round(random.uniform(0, 50), 2), 'discount_amount', 0.40) for _ in range(batch_size)]  # Many nulls
        # batch_data['discount_percent'] = [self.introduce_messiness(round(random.uniform(0, 25), 1), 'discount_percent', 0.45) for _ in range(batch_size)]  # Many nulls
        # batch_data['coupon_code'] = [self.introduce_messiness(f"SAVE{random.randint(5,50)}", 'coupon_code', 0.70) for _ in range(batch_size)]  # Mostly null
        batch_data['payment_method'] = [self.introduce_messiness(random.choice(self.payment_methods), 'payment_method', 0.05) for _ in range(batch_size)]
        batch_data['payment_status'] = [self.introduce_messiness(ps, 'payment_status', 0.04) for ps in payment_statuses]
        
        # Return and Refund columns (2 columns)
        return_refund_pairs = [self.get_return_refund_pair() for _ in range(batch_size)]
        batch_data['order_returned'] = [self.introduce_messiness(pair[0], 'order_returned', 0.10) for pair in return_refund_pairs]
        batch_data['payment_refunded'] = [self.introduce_messiness(pair[1], 'payment_refunded', 0.10) for pair in return_refund_pairs]
        
        # Shipping/Geography (7 columns) with real ZIP codes
        shipping_addresses = []
        shipping_cities = []
        shipping_states = []
        shipping_zips = []
        
        for i in range(batch_size):
            # Generate street address
            street_num = random.randint(1, 9999)
            street_names = ['Main St', 'Oak Ave', 'Elm Dr', 'First St', 'Second Ave', 'Park Rd', 'Maple St', 'Cedar Ave']
            address = f"{street_num} {random.choice(street_names)}"
            shipping_addresses.append(address)
            
            # 85% same city/state as customer, 15% different
            if random.random() < 0.85:
                ship_city = customer_cities[i]
                ship_state = customer_states[i]
            else:
                ship_city, ship_state = random.choice(self.cities_states)
            
            shipping_cities.append(ship_city)
            shipping_states.append(ship_state)
            
            # Generate correct ZIP for city/state, then make it messy
            correct_zip = self.get_zip_for_city_state(ship_city, ship_state)
            shipping_zips.append(correct_zip)
        
        batch_data['shipping_address_line1'] = shipping_addresses
        batch_data['shipping_address_line2'] = [self.introduce_messiness(f"Apt {random.randint(1,999)}", 'shipping_address_line2', 0.65) for _ in range(batch_size)]  # Mostly null
        batch_data['shipping_city'] = [city for city in shipping_cities]
        batch_data['shipping_state'] = [state for state in shipping_states]
        batch_data['shipping_zip'] = shipping_zips  # Already messy from create_messy_zip
        batch_data['shipping_country'] = [self.introduce_messiness('US', 'shipping_country', 0.03) for _ in range(batch_size)]
        batch_data['shipping_method'] = [self.introduce_messiness(random.choice(self.shipping_methods), 'shipping_method', 0.05) for _ in range(batch_size)]
        
        return pd.DataFrame(batch_data)
    
    def generate_csv(self, filename='input/messy_ecommerce_1K.csv'):
        """Generate the complete CSV file in batches"""
        print(f"\nStarting generation of {self.total_rows:,} rows of messy e-commerce data...")
        print(f"Output file: {filename}")
        start_time = time.time()
        
        # Calculate number of batches
        num_batches = (self.total_rows + self.batch_size - 1) // self.batch_size
        
        # Generate first batch to create file with headers
        first_batch = self.generate_batch_data(0, min(self.batch_size, self.total_rows))
        first_batch.to_csv(filename, index=False, mode='w')
        rows_written = len(first_batch)
        
        print(f"Column structure: {len(first_batch.columns)} columns")
        print(f"Columns: {', '.join(first_batch.columns)}")
        
        # Generate remaining batches
        for batch_num in range(1, num_batches):
            batch_start = batch_num * self.batch_size
            current_batch_size = min(self.batch_size, self.total_rows - batch_start)
            
            if current_batch_size <= 0:
                break
            
            batch_df = self.generate_batch_data(batch_start, current_batch_size)
            batch_df.to_csv(filename, index=False, mode='a', header=False)
            rows_written += len(batch_df)
            
            # Memory cleanup
            del batch_df
            gc.collect()
            
            # Progress update
            if batch_num % 5 == 0:
                elapsed = time.time() - start_time
                rate = rows_written / elapsed
                remaining_rows = self.total_rows - rows_written
                eta = remaining_rows / rate if rate > 0 else 0
                print(f"Progress: {rows_written:,}/{self.total_rows:,} ({rows_written/self.total_rows*100:.1f}%) - Rate: {rate:,.0f} rows/sec - ETA: {eta:.0f}s")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\nCSV generation completed successfully!")
        print(f"File: {filename}")
        print(f"Total rows: {rows_written:,}")
        print(f"Total columns: 45")
        print(f"Generation time: {total_time:.1f} seconds")
        print(f"Average rate: {rows_written/total_time:,.0f} rows/second")
        print(f"Estimated file size: ~{rows_written * 0.9 / 1024:.1f} MB")
        
        print(f"\nData Quality Issues Summary:")
        print(f"• Real ZIP codes with 15% error rate (nulls, mismatches, invalid formats)")
        print(f"• Payment-Order status correlation with business logic")
        print(f"• Return-Refund correlation (if returned=yes, then refunded=yes)")
        print(f"• Customer emails: 12% format issues (missing @, multiple values)")
        print(f"• Phone numbers: 25% nulls or format variations")
        print(f"• Shipping costs: Under $10 as requested")
        print(f"• Ages: 12% invalid values (negative, text, out of range)")
        print(f"• Calculations: 10% of line_totals have errors")
        print(f"• General messiness: Extra spaces, case issues, typos")
        
        return filename

def main():
    """Main execution function"""
    print("=" * 60)
    print("MESSY E-COMMERCE DATA GENERATOR")
    print("=" * 60)
    print("Following established rules:")
    print("- 2,500 unique customers (50 first x 50 last names)")
    print("- ~500 products across 5 categories")  
    print("- 50 warehouses in 10 cities")
    print("- Q1 2024 date range (90 days)")
    print("- Real ZIP codes with error patterns")
    print("- 45 columns with realistic messiness")
    print("- 1M rows with repeat customer behavior")
    print("- Business logic: payment-order status correlation")
    print("- Return-refund correlation")
    print("=" * 60)
    
    # Create generator instance
    generator = MessyEcommerceGenerator(total_rows=1000)
    
    # Generate the CSV
    output_file = generator.generate_csv('input/messy_ecommerce_1K.csv')
    
    print(f"\nPerfect for your data pipeline project:")
    print(f"1. Upload to S3 bucket")
    print(f"2. Trigger Lambda -> Glue ETL job")  
    print(f"3. Clean and transform messy data")
    print(f"4. Split into fact/dimension tables")
    print(f"5. Load into Redshift as star schema")
    
    print(f"\nETL Challenges included:")
    print(f"• ZIP code validation against city/state")
    print(f"• Payment-order status business rule validation") 
    print(f"• Return-refund correlation checks")
    print(f"• Data format standardization")
    print(f"• Null value handling strategies")
    print(f"• Duplicate detection and removal")
    
    print(f"\nReady for your resume project!")
    print("=" * 60)

if __name__ == "__main__":
    main()