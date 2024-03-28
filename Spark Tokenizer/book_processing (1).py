from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, regexp_replace, explode, split, count

if __name__ == "__main__":
    # SparkSession oluştur
    spark = SparkSession.builder.appName("BookProcessing").getOrCreate()

    # 1. book.txt dosyasını oku ve tokenize et
    book_data = spark.read.text("book.txt") \
        .select(explode(split(col("value"), "\s+")).alias("word"))

    # 2. Ayraçları filtrele [a-z][0-9]' işlemini gerçekleştir
    book_data = book_data.withColumn("word", regexp_replace("word", "[^a-z0-9']", ""))

    # 3. Lowercase dönüşümü
    book_data = book_data.withColumn("word_lowercase", lower("word"))

    

    # 4. GroupBy ve Count işlemleri
    word_counts = book_data.groupBy("word_lowercase").agg(count("*").alias("count"))

    # 5. Collect üzerinde iterator ile dönüş, 6. Print kullanarak her kelime için ne kadar o metinde geçtiğini göster
    print("Word Counts:")
    word_counts_collect = word_counts.collect()
    for row in word_counts_collect:
        print(f"{row['word_lowercase']}: {row['count']}")

    # Sonuçları bir dosyaya yazdır
    with open("final.txt", "w") as f:
                
        f.write("\nWord Counts:\n")
        for row in word_counts_collect:
            f.write(f"{row['word_lowercase']}: {row['count']}\n")

    # SparkSession'ı kapat
    spark.stop()
