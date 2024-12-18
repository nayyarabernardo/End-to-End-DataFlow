
# **End to End DataFlow**

## **Project Overview**

Este projeto visa otimizar campanhas de marketing de um banco, utilizando análises avançadas e modelos preditivos para entender melhor os fatores que influenciam os clientes a aderirem a depósitos bancários. Com insights baseados em características demográficas, histórico de campanhas e indicadores econômicos, o projeto ajuda a direcionar estratégias de marketing mais eficientes.

---

## **Objetivos do Negócio**



---

## **Arquitetura do Projeto**

A arquitetura do projeto segue um pipeline em três etapas principais:

1. **Ingestão de Dados**  
   Os dados de campanhas anteriores são carregados para a camada **trusted** no Google BigQuery.

2. **Modelagem e Análise**  
   Um modelo de regressão logística é treinado para prever a probabilidade de adesão a um depósito, gerando uma tabela **delivery** com os resultados.

3. **Visualização e Insights**  
   Um dashboard no Looker Studio apresenta insights detalhados, incluindo análises demográficas, padrões temporais e eficácia de campanhas.

---

## **Estrutura de Pastas**

```plaintext
bank-marketing-analytics/
├── data/
│   ├── raw/                    # Arquivos brutos para ingestão inicial.
│   └── processed/              # Dados pré-processados para análise e modelagem.
├── scripts/
│   ├── ingestion.py            # Script para ingestão de dados no GCP.
│   ├── training_pipeline.py    # Script para treinar o modelo de regressão logística.
│   ├── prediction.py           # Script para gerar previsões.
│   └── airflow_dag.py          # DAG do Airflow para orquestrar o pipeline.
├── models/
│   └── logistic_regression.pkl # Modelo treinado.
├── docs/
│   ├── trusted_table.md        # Documentação da tabela trusted.
│   ├── delivery_table.md       # Documentação da tabela delivery.
│   └── model_documentation.md  # Documentação do modelo.
├── dashboards/
│   └── looker_studio/          # Arquivos relacionados ao dashboard no Looker Studio.
├── README.md                   # Documentação principal.
└── requirements.txt            # Dependências do projeto.
```

---

## **Camadas de Dados**

### **1. Tabela Trusted (`trusted.bank_data`)**  
A camada **trusted** contém os dados brutos validados, prontos para análise e modelagem.

**Esquema:**
- **age** (INT): Idade do cliente.
- **job** (STRING): Tipo de emprego.
- **marital** (STRING): Estado civil.
- **education** (STRING): Nível de educação.
- **contact** (STRING): Canal de contato (celular, telefone fixo).
- **previous_outcome** (STRING): Resultado da campanha anterior.
- **euribor_rate** (FLOAT): Taxa Euribor no momento do contato.
- **y** (BOOLEAN): Indicador de adesão ao depósito (sim/não).

---

### **2. Tabela Delivery (`delivery.marketing_predictions`)**  
A camada **delivery** apresenta as previsões geradas pelo modelo, com campos-chave para análise detalhada.

**Esquema:**
- **user_id** (STRING): Identificação do cliente (CPF ou ID único).
- **score** (FLOAT): Score atribuído pelo modelo ao cliente.
- **scoring_value** (FLOAT): Valor calculado para escoragem.
- **approval_probability** (FLOAT): Probabilidade estimada de adesão ao depósito.
- **prediction** (STRING): Previsão do status (sim/não).
- **inclusion_date** (DATE): Data de inclusão do registro.

**Uso:**  
A tabela **delivery** é a base para visualizações no dashboard, permitindo a análise de resultados e eficácia das campanhas.

---

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

## **Instalação e Execução**

### **Pré-requisitos**
- Conta no Google Cloud Platform (GCP).
- Python 3.8 ou superior.
- Pacotes listados em `requirements.txt`.

### **Passos**
1. Clone o repositório:
   ```bash
   git clone https://github.com/nayyarabernardo/End-to-End-DataFlow.git

   cd End-to-End-DataFlow
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

4. Execute os scripts:
   - Ingestão de dados:
     ```bash
     python scripts/ingestion.py
     ```
   - Treinamento do modelo:
     ```bash
     python scripts/training_pipeline.py
     ```
   - Geração de previsões:
     ```bash
     python scripts/prediction.py
     ```

---

## **Dashboard**

O dashboard está disponível [aqui](https://lookerstudio.google.com/reporting/53ba542b-9439-416a-a2f9-9e5ac1eb7f8f)
- Insights demográficos sobre clientes.
- Probabilidades de adesão a depósitos.
- Comparação de eficácia de campanhas e canais de contato.

---

## **Erro e Solução de Problemas**

### **Erros Comuns**
1. **Credenciais do GCP ausentes:**
   - Certifique-se de que o arquivo `key.json` está no diretório correto.
   - Exporte a variável de ambiente novamente.

