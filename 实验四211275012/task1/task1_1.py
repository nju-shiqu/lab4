from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor

# 创建 SparkSession
spark = SparkSession.builder.appName("LoanAmountDistribution").getOrCreate()

# 读取 CSV 文件
df = spark.read.csv("file:///home/gyy/application_data.csv", header=True, inferSchema=True)

# 选择并处理贷款金额列
df = df.withColumn("AMT_CREDIT", col("AMT_CREDIT").cast("double"))  # 确保列为浮点型
df = df.withColumn("Credit_Range", floor(col("AMT_CREDIT") / 10000) * 10000)

# 计算每个区间的贷款金额分布
distribution = df.groupBy("Credit_Range").count()

# 格式化输出
formatted_output = distribution.rdd.map(lambda x: ((x[0], x[0] + 10000), x[1]))
# 对结果进行排序
sorted_output = formatted_output.sortBy(lambda x: x[0])

# 重分区到一个分区
single_partition_rdd = sorted_output.coalesce(1)
sorted_output.collect()
# 保存到文本文件
output_path = "file:///home/gyy/lab4/output1-1"
single_partition_rdd.saveAsTextFile(output_path)

# 重分区到一个分区
single_partition_rdd = sorted_output.coalesce(1)

# 停止 Spark Session
spark.stop()