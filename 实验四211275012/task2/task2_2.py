from pyspark.sql import SparkSession
from pyspark.sql.functions import abs, col, format_number

# 初始化Spark会话
spark = SparkSession.builder.appName("AverageDailyIncome").getOrCreate()

# 读取 CSV 文件
df = spark.read.csv("file:///home/gyy/application_data.csv", header=True, inferSchema=True)

# 计算每天的平均收入，并使用format_number保留五位小数
df = df.withColumn("DAYS_BIRTH_ABS", abs(col("DAYS_BIRTH")))
df = df.withColumn("avg_income", col("AMT_INCOME_TOTAL") / col("DAYS_BIRTH_ABS"))
# 筛选出每日收入大于1的客户
filtered_df = df.filter(col("avg_income") > 1)
# 按照每日平均收入降序排序
sorted_df = filtered_df.orderBy(col("avg_income").desc())

# 选择需要的列
df_final = sorted_df.select("SK_ID_CURR", "avg_income")

# 将结果汇总到一个csv文件中
df_final.coalesce(1).write.csv("file:///home/gyy/lab4/ouput2-2", header=True)

# 关闭Spark会话
spark.stop()
