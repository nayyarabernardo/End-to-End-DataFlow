import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime

# Inicializar Faker
fake = Faker()

def gerar_departments(num_records=10):
    data = {
        'department_id': range(1, num_records + 1),
        'department_name': [fake.word().capitalize() + ' Department' for _ in range(num_records)]
    }
    return pd.DataFrame(data)

def gerar_categories(num_records=20):
    data = {
        'category_id': range(1, num_records + 1),
        'category_department_id': np.random.randint(1, 11, num_records),
        'category_name': [fake.word().capitalize() + ' Category' for _ in range(num_records)]
    }
    return pd.DataFrame(data)

def gerar_orders(num_records=50):
    data = {
        'order_id': range(1, num_records + 1),
        'order_date': [fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d') for _ in range(num_records)],
        'order_customer_id': np.random.randint(1, 101, num_records),
        'order_status': np.random.choice(['Pending', 'Completed', 'Shipped', 'Cancelled'], num_records)
    }
    return pd.DataFrame(data)

def gerar_products(num_records=30):
    data = {
        'product_id': range(1, num_records + 1),
        'product_category_id': np.random.randint(1, 21, num_records),
        'product_name': [fake.word().capitalize() + ' Product' for _ in range(num_records)],
        'product_description': [fake.sentence() for _ in range(num_records)],
        'product_price': np.round(np.random.uniform(10, 500, num_records), 2),
        'product_image': [fake.image_url() for _ in range(num_records)]
    }
    return pd.DataFrame(data)

def gerar_customers(num_records=100):
    data = {
        'customer_id': range(1, num_records + 1),
        'customer_fname': [fake.first_name() for _ in range(num_records)],
        'customer_lname': [fake.last_name() for _ in range(num_records)],
        'customer_email': [fake.email() for _ in range(num_records)],
        'customer_password': [fake.password() for _ in range(num_records)],
        'customer_street': [fake.street_address() for _ in range(num_records)],
        'customer_city': [fake.city() for _ in range(num_records)],
        'customer_state': [fake.state() for _ in range(num_records)],
        'customer_zipcode': [int(fake.postcode()) for _ in range(num_records)]
    }
    return pd.DataFrame(data)

def gerar_order_items(num_records=100):
    data = {
        'order_item_id': range(1, num_records + 1),
        'order_item_order_id': np.random.randint(1, 51, num_records),
        'order_item_product_id': np.random.randint(1, 31, num_records),
        'order_item_quantity': np.random.randint(1, 10, num_records),
        'order_item_subtotal': np.round(np.random.uniform(10, 500, num_records), 2),
        'order_item_product_price': np.round(np.random.uniform(10, 200, num_records), 2)
    }
    return pd.DataFrame(data)

def main():
    # Gerar data e hora atual para nome dos arquivos
    data_hora_atual = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Dicionário de funções de geração de dados
    geradores = {
        'departments': gerar_departments,
        'categories': gerar_categories,
        'orders': gerar_orders,
        'products': gerar_products,
        'customers': gerar_customers,
        'order_items': gerar_order_items
    }
    
    # Caminho onde os arquivos serão salvos
    caminho_salvar = "../data/retail_db_fake/"
    
    # Gerar e salvar cada tabela
    for nome_tabela, gerador in geradores.items():
        df = gerador()
        filename = f"{caminho_salvar}{nome_tabela}_mockados_{data_hora_atual}.parquet"
        df.to_parquet(filename, index=False)
        print(f"Gerado {filename}")

if __name__ == "__main__":
    main()
