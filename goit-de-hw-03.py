from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

# Step 1: Initialize Spark Session
# Крок 1: Ініціалізація сесії Spark
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Step 2: Load CSV files into DataFrames
# Крок 2: Завантаження CSV-файлів у DataFrame
users_data = spark.read.csv(r'C:\Users\dell\PycharmProjects\sandbox_pyspark_goitt\users.csv', header=True, inferSchema=True)
purchases_data = spark.read.csv(r'C:\Users\dell\PycharmProjects\sandbox_pyspark_goitt\purchases.csv', header=True, inferSchema=True)
products_data = spark.read.csv(r'C:\Users\dell\PycharmProjects\sandbox_pyspark_goitt\products(1).csv', header=True, inferSchema=True)

# Перевірка завантажених даних
print("Users Data:")
users_data.show(5)  # Вивести перші 5 рядків

print("Purchases Data:")
purchases_data.show(5)  # Вивести перші 5 рядків

print("Products Data:")
products_data.show(5)  # Вивести перші 5 рядків

# Step 3: Drop rows with missing values
# Крок 3: Видалення рядків із пропущеними значеннями
users_data = users_data.dropna()
purchases_data = purchases_data.dropna()
products_data = products_data.dropna()

# Step 4: Calculate the total purchase amount for each product category
# Крок 4: Визначення загальної суми покупок за кожною категорією продуктів
combined_data = purchases_data.join(products_data, "product_id", "inner") \
                              .join(users_data, "user_id", "inner")

category_total_data = combined_data.groupBy("category") \
                                   .agg(sum(col("quantity") * col("price")).alias("total_purchase"))

# Show total purchase amount by category
# Показати загальну суму покупок за категорією
print("Total purchase by category:")
category_total_data.show()

# Step 5: Calculate the total purchase amount for age group 18-25 by category
# Крок 5: Визначення суми покупок для вікової категорії 18-25 за кожною категорією продуктів
age_group_data = combined_data.filter((col("age") >= 18) & (col("age") <= 25)) \
                              .groupBy("category") \
                              .agg(sum(col("quantity") * col("price")).alias("total_purchase_18_25"))

# Show purchase amount by category for age group 18-25
# Показати суму покупок за категорією для вікової групи 18-25
print("Total purchase by category for age 18-25:")
age_group_data.show()

# Step 6: Calculate the percentage of purchases by category for age group 18-25
# Крок 6: Визначення частки покупок за кожною категорією товарів для вікової категорії 18-25
total_purchase_18_25 = age_group_data.agg(sum("total_purchase_18_25").alias("grand_total")).collect()[0]["grand_total"]

percentage_data = age_group_data.withColumn(
    "percentage",
    round((col("total_purchase_18_25") / total_purchase_18_25) * 100, 2)
)

# Show percentage of purchases by category for age group 18-25
# Показати відсоток покупок за категорією для вікової групи 18-25
print("Percentage of purchases by category for age 18-25:")
percentage_data.show()

# Step 7: Find the top 3 categories with the highest percentage
# Крок 7: Визначення 3 категорій з найвищим відсотком покупок
top_categories_data = percentage_data.orderBy(col("percentage").desc()).limit(3)

# Show top 3 categories
# Показати топ-3 категорій
print("Top 3 categories with the highest percentage:")
top_categories_data.show()


# Step 8: Stop the Spark session
# Крок 8: Завершення сесії Spark
spark.stop()
