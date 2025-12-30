import duckdb

# 1. Открываем файл
con = duckdb.connect()

# 2. Читаем структуру
result = con.execute("SELECT unnest(products) FROM 'D:/googl_loads/products.parquet' limit 20 ").df()


# 3. Выводим
print("СТОЛБЦЫ:", result.columns)
print(result.head(5))  # Показать первые 5 строк