# Credit Card Fraud Detection Pipeline

Batch processing pipeline for fraud detection over 284,807 real credit card transactions, built with Apache Spark and Scala.

## Dataset

[Kaggle - Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud)

- 284,807 transactions over two days
- 492 fraud cases (0.17% of total)
- Features V1-V28 are PCA-transformed for anonymization
- `Amount`: transaction value, `Class`: 0 = legitimate, 1 = fraud

## Pipeline

1. Reads raw CSV data into a Spark DataFrame
2. Separates fraudulent from legitimate transactions
3. Computes descriptive statistics on fraud amounts
4. Classifies fraud by amount range (low / medium / high)
5. Writes results to Parquet format

## Results

| Range       | Cases | Avg Amount |
|-------------|-------|------------|
| low (<100)  | 362   | 19.39      |
| medium      | 121   | 329.52     |
| high (>1000)| 9     | 1470.81    |

Most fraud occurs at low amounts — consistent with card-testing behavior.

## Stack

- Apache Spark 3.5.1
- Scala 2.12.18
- Java 17
- Output: Parquet

## Run
```bash
spark-shell
:load src/main/scala/FinancialPipeline.scala
FinancialPipeline.main(Array())
```
