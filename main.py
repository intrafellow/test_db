from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def get_product_category_pairs(products_df, categories_df, product_category_df):
    # Соединение продуктов с категориями
    product_category_left = products_df.join(product_category_df, on="product_id", how="left")
    product_category_full = product_category_left.join(categories_df, on="category_id", how="left")

    # Пары продукт-категория
    product_category_pairs = product_category_full.select("product_name", "category_name")

    # Продукты без категорий
    products_without_categories = product_category_full.filter(col("category_id").isNull()) \
        .select("product_name") \
        .withColumn("category_name", lit(None))

    # Объединение пар и продуктов без категорий
    result = product_category_pairs.union(products_without_categories)

    return result


# Инициализация данных
spark = SparkSession.builder.appName("ProductsCategories").getOrCreate()

products_data = [(1, "Product A"), (2, "Product B"), (3, "Product C")]
categories_data = [(1, "Category X"), (2, "Category Y")]
product_category_data = [(1, 1), (2, 2)]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

# Вызов метода и отображение результата
result_df = get_product_category_pairs(products_df, categories_df, product_category_df)
result_df.show()
