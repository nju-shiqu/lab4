from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 初始化Spark会话
spark = SparkSession.builder.appName("LoanIncomeDifference").getOrCreate()

#读取 CSV 文件
df = spark.read.csv("file:///home/gyy/application_data.csv", header=True, inferSchema=True)

# 计算差值
df = df.withColumn("Difference", col("AMT_CREDIT") - col("AMT_INCOME_TOTAL"))

# 获取差值最高的十条记录
top10max = df.orderBy(col("Difference").desc()).limit(10)

# 获取差值最低的十条记录
top10min = df.orderBy(col("Difference")).limit(10)


top1=top10max.select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "Difference")
top2=top10min.select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "Difference")

# 保存到文本文件
output_path1 = "file:///home/gyy/lab4/output1-2high"
top1.saveAsTextFile(output_path1)


output_path2 = "file:///home/gyy/lab4/output1-2low"
top2.saveAsTextFile(output_path2)




# 关闭Spark会话
spark.stop()
