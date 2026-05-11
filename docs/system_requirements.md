# Elicitação de Requisitos – Teste Técnico BeFly (PySpark + Medalhão)

| Feitor por:  | Contate-me pelo email: | Desenvolvido em: |
| -------------| -----------------------| -----------------|
| Vitor Duarte | vitor02hugo@alu.ufc.br | 11/05/2026       |


Neste documento estão os requisitos extraídos da descrição do case de forma organizada, separando **funcionais** (o que o sistema deve fazer) e **não funcionais** (como deve fazer / restrições de qualidade).

---

## 1. Requisitos Funcionais (RF)

### 🥉 RF – Camada Bronze (Ingestão)

| ID | Requisito |
|---|---|
| RF-B01 | Ler 3 arquivos CSV: `hotel_bookings.csv`, `country_metadata.csv`, `hotel_metadata.csv` |
| RF-B02 | Configurar leitura com `header=True`, separador `,`, e inferência ou schema explícito |
| RF-B03 | Tratar a string literal `"NULL"` como valor nulo (em `agent`, `company`, `country`) |
| RF-B04 | Persistir os 3 datasets em **Parquet com compressão Snappy** em `data/bronze/{bookings, countries, hotels}/` |
| RF-B05 | (Bônus) Particionar `bookings` por `arrival_date_year` |
| RF-B06 | (Opcional) Aplicar recorte de volume documentado (ano, hotel ou amostragem) |

### 🥈 RF – Camada Silver (Limpeza + Enriquecimento)

| ID | Requisito |
|---|---|
| RF-S01 | Converter colunas numéricas para `integer` e `adr` para `double` |
| RF-S02 | Converter `reservation_status_date` para `date` |
| RF-S03 | Criar coluna `arrival_date` (date) a partir do ano + mês textual + dia |
| RF-S04 | Substituir nulos em `children` por `0` |
| RF-S05 | Substituir nulos em `country` por `"UNK"` |
| RF-S06 | Filtrar reservas sem hóspedes (`adults+children+babies = 0`) |
| RF-S07 | Filtrar registros com `adr < 0` |
| RF-S08 | Logar quantidade de registros removidos por filtro |
| RF-S09 | Criar **no mínimo 3 colunas derivadas**: `total_nights`, `total_guests`, `is_family`, `revenue`, `booking_status`, `is_long_stay` |
| RF-S10 | LEFT JOIN com `country_metadata` (trazer `country_name`, `continent`) |
| RF-S11 | LEFT JOIN com `hotel_metadata` (trazer `hotel_city`, `hotel_star_rating`, `hotel_opened_year`) |
| RF-S12 | Garantir 1 linha por reserva (sem duplicação por joins) |
| RF-S13 | Persistir Silver em Parquet em `data/silver/bookings_enriched/` |
| RF-S14 | (Bônus) Particionar Silver por `arrival_date_year` (+ mês) |

### 🥇 RF – Camada Gold (Visões Analíticas)

| ID | Requisito |
|---|---|
| RF-G01 | Criar **pelo menos 1 visão agregada** (idealmente 2–3) |
| RF-G02 | Visão sugerida 1: **Receita/ocupação por hotel × mês** (total_bookings, cancelled_bookings, total_revenue, avg_adr, total_nights_sold, cancellation_rate) |
| RF-G03 | Visão sugerida 2: **Cancelamentos por segmento** (market_segment × customer_type × distribution_channel) |
| RF-G04 | Visão sugerida 3: **Top 20 países por receita** |
| RF-G05 | Visão sugerida 4: **Estadia média e perfil de hóspedes** |
| RF-G06 | Usar API DataFrame (`groupBy` + `agg`) — **não SQL puro** |
| RF-G07 | Persistir em `data/gold/` em Parquet ou CSV com nomes autoexplicativos |

### 📦 RF – Entrega

| ID | Requisito |
|---|---|
| RF-E01 | Estrutura de pastas organizada (`scripts/`, `data/{raw,bronze,silver,gold}/`) |
| RF-E02 | README com visão geral, instruções de execução, recorte aplicado, transformações e perguntas respondidas |
| RF-E03 | Comentário arquitetural (1–2 parágrafos) sobre adaptação para AWS/Databricks |

---

## 2. Requisitos Não Funcionais (RNF)

### 🛠 Tecnologia & Plataforma
| ID | Requisito |
|---|---|
| RNF-T01 | Usar **Python + PySpark (DataFrame API)** |
| RNF-T02 | Execução **local** (`pyspark`, `spark-submit`, Spark local) |
| RNF-T03 | I/O em filesystem local (CSV → Parquet) |
| RNF-T04 | Formato analítico padrão: **Parquet + Snappy** |

### ⚡ Desempenho & Escalabilidade
| ID | Requisito |
|---|---|
| RNF-P01 | Pipeline deve rodar em laptop modesto (~200MB RAM para 119k linhas) |
| RNF-P02 | Particionamento por ano para acelerar queries temporais |
| RNF-P03 | Tempo total de implementação: **até 2 horas** |

### 🧱 Arquitetura
| ID | Requisito |
|---|---|
| RNF-A01 | Seguir estritamente o padrão **Medalhão (Bronze → Silver → Gold)** |
| RNF-A02 | Separação física por diretórios (`data/bronze/`, `data/silver/`, `data/gold/`) |
| RNF-A03 | Bronze deve preservar o schema original (sem regra de negócio) |
| RNF-A04 | Silver deve ser idempotente e reprocessável |
| RNF-A05 | Gold deve ter granularidade reduzida e foco em KPIs |

### 📐 Qualidade de Código
| ID | Requisito |
|---|---|
| RNF-Q01 | Nomes de variáveis e colunas claros e autoexplicativos |
| RNF-Q02 | Separação lógica das etapas em scripts distintos |
| RNF-Q03 | Comentários pontuais onde necessário |
| RNF-Q04 | Logs informativos (ex.: registros removidos) |
| RNF-Q05 | (Bônus) Schema explícito ao invés de inferSchema |

### 📊 Qualidade de Dados
| ID | Requisito |
|---|---|
| RNF-D01 | Tratamento consistente de nulos (documentado) |
| RNF-D02 | Tipagem correta em todas as colunas |
| RNF-D03 | Sem duplicação de registros após joins |
| RNF-D04 | Filtros de invalidação documentados |

### 📚 Documentação
| ID | Requisito |
|---|---|
| RNF-DOC01 | README contendo: versões, dependências, comandos de execução |
| RNF-DOC02 | Documentar recorte adotado |
| RNF-DOC03 | Mapear cada visão Gold à pergunta de negócio que responde |
| RNF-DOC04 | Reflexão arquitetural sobre nuvem (AWS/Databricks): S3 layout, Glue, Athena, orquestração, qualidade, governança |

---

## 3. Perguntas de Negócio a Responder (drivers das visões Gold)

1. Quanto cada hotel fatura mês a mês?
2. Quais segmentos/canais cancelam mais? Com que antecedência reservam?
3. De que países vêm os hóspedes mais valiosos?
4. Como detectar reservas com alto risco de cancelamento?

---
