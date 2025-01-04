# **Retail Analytics and Prediction System**

## **Visão Geral do Projeto**

Este projeto visa criar um sistema robusto de análise e previsão para o setor de varejo. Utilizando pipelines de dados, modelos preditivos e visualizações, o sistema ajuda a otimizar processos e melhorar a tomada de decisões baseada em dados.

---

## **Objetivos do Negócio**

- Automatizar a ingestão de dados de diferentes fontes para o data lake no Google Cloud.
- Criar um data warehouse escalável no BigQuery com tabelas confiáveis (trusted) e processadas (work).
- Desenvolver um modelo preditivo para insights operacionais e estratégicos.
- Fornecer dashboards dinâmicos para visualização de KPIs relevantes.

---

## **Arquitetura do Projeto**

O sistema segue as seguintes etapas principais:

1. **Ingestão de Dados**

   - Scripts para transferência de arquivos CSV para buckets no Google Cloud Storage.
   - Ingestão desses arquivos em tabelas RAW no BigQuery.

2. **Processamento de Dados**

   - Transformação e carregamento de dados para tabelas TRUSTED e WORK usando dbt.
   - Criação de uma tabela DELIVERY consolidada para insights.

3. **Orquestração**

   - DAGs no Apache Airflow para executar o pipeline automaticamente.

4. **Visualização**

   - Dashboards no Looker Studio para apresentar KPIs e análises detalhadas.

---

## **Estrutura de Diretórios**

```plaintext
retail-analytics/
├── data/
│   ├── raw/                      # Dados brutos para ingestão inicial.
├── scripts/
│   ├── ingestion_bucket/
│   │   ├── ingestion_bucket_retail_categories.py
│   │   ├── ingestion_bucket_retail_customers.py
│   │   ├── ingestion_bucket_retail_departments.py
│   │   ├── ingestion_bucket_retail_order_items.py
│   │   ├── ingestion_bucket_retail_order_v2.py
│   │   └── ingestion_bucket_retail_products.py
│   ├── ingestion_table/
│   │   ├── ingestion_table_retail_categories.py
│   │   ├── ingestion_table_retail_customers.py
│   │   ├── ingestion_table_retail_departments.py
│   │   ├── ingestion_table_retail_order.py
│   │   ├── ingestion_table_retail_order_items.py
│   │   └── ingestion_table_retail_products.py
├── dags/
│   ├── ingestion_retail_categories_dag.py
│   ├── ingestion_retail_departments_dag.py
│   └── ingestion_table_retail.py
├── models/
│   ├── sources/
│   │   └── scr_db_retail.yml
│   ├── staging/
│   │   ├── stg_db_retail_trusted_categories.sql
│   │   ├── stg_db_retail_trusted_customers.sql
│   │   ├── stg_db_retail_trusted_departments.sql
│   │   ├── stg_db_retail_trusted_order_items.sql
│   │   └── stg_db_retail_trusted_products.sql
├── dashboards/
│   └── looker_studio/          # Dashboards no Looker Studio.
├── README.md                   # Documentação principal.
└── requirements.txt            # Dependências do projeto.
```

---

## **Camadas de Dados**

### **Data Lake (Google Cloud Storage)**

Os arquivos CSV são armazenados no formato hierárquico por tipo de dados e timestamp:

```plaintext
gs://ingestion-raw-data-retail/categories/categories_YYYYMMDDHHMMSS
gs://ingestion-raw-data-retail/customers/customers_YYYYMMDDHHMMSS
gs://ingestion-raw-data-retail/departments/departments_YYYYMMDDHHMMSS
gs://ingestion-raw-data-retail/order_items/order_items_YYYYMMDDHHMMSS
gs://ingestion-raw-data-retail/products/products_YYYYMMDDHHMMSS
```

### **Data Warehouse (BigQuery)**

As tabelas são organizadas nas camadas RAW, TRUSTED e WORK:

- **RAW:** Dados brutos importados diretamente do GCS.
- **TRUSTED:** Dados validados e prontos para análise.
- **WORK:** Dados processados para modelagem e relatórios.

Exemplo de tabelas:

- `raw_categories`, `trusted_categories`
- `raw_customers`, `trusted_customers`
- `raw_departments`, `trusted_departments`
- `raw_order_items`, `trusted_order_items`
- `raw_products`, `trusted_products`

---

## **Orquestração com Apache Airflow**

As DAGs do Airflow controlam a execução automatizada do pipeline. Exemplos de DAGs:

- `ingestion_retail_categories_dag.py`
- `ingestion_retail_departments_dag.py`
- `ingestion_table_retail.py`

---

## **Pipeline de Machine Learning (Em Desenvolvimento)**

- A tabela DELIVERY será criada com dbt, consolidando dados de todas as tabelas TRUSTED.
- Aplicação de modelos preditivos para insights avançados.
- Execução da lógica do dbt no Airflow para integração total do pipeline.

## **Modelo de Machine Learning**

### **Modelo: Regressão Logística**

#### **Objetivo:**
Prever a probabilidade de um cliente aderir a um depósito bancário com base em suas características e histórico.

#### **Documentação do Modelo:**
O modelo é detalhado no arquivo `docs/model_documentation.md`, incluindo:
- Descrição do problema.
- Pré-processamento dos dados.
- Métricas de avaliação (Acurácia, F1-Score, Recall).
- Hiperparâmetros ajustados.

---

## **Dashboard**

O dashboard será implementado no Looker Studio para visualizações como:

- Análises demográficas e comportamentais de clientes.
- Monitoramento de vendas e estoque.
- Indicadores de desempenho do varejo.

---

## **Instalação e Execução**

### **Pré-requisitos**

- Conta no Google Cloud Platform (GCP).
- Python 3.8 ou superior.
- Dependências listadas em `requirements.txt`.

### **Passos**

1. Clone o repositório:

   ```bash
   git clone https://github.com/nayyarabernardo/Retail-Analytics.git
   cd Retail-Analytics
   ```

2. Instale as dependências:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure as credenciais do GCP:

   - Coloque o arquivo `key.json` no diretório raiz.
   - Exporte a variável de ambiente:
     ```bash
     export GOOGLE_APPLICATION_CREDENTIALS="key.json"
     ```

4. Execute os scripts de ingestão:

   ```bash
   python scripts/ingestion_bucket/ingestion_bucket_retail_categories.py
   ```

---

## **Próximos Passos**

- Finalizar o esquema da tabela DELIVERY.
- Testar e implantar a orquestração completa no Airflow.
- Migrar o pipeline para uma máquina virtual no GCP.

