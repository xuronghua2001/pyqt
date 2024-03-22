import findspark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F



findspark.init()

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    df = spark.read.csv('input/sql/u.data', sep='	', header=False)
    df2 = df.toDF("uid", "mid", "score","timestamp")
    # df2.printSchema()
    # df2.show()
    df2.createTempView("score")
    print("1 查询用户平均分")
    df2 = df2.withColumn("score", df2["score"].cast("int"))
    spark.sql("""
        SELECT uid, AVG(score) AS average_score FROM score GROUP BY uid;
        """).show()
    # df3 = df2.groupBy("uid").agg(F.avg("score").alias("avg_score"))
    # df3.show()
    print("2 查询电影平均分")
    spark.sql("""
            SELECT mid, AVG(score) AS average_score FROM score GROUP BY mid;
            """).show()
    # df4 = df2.groupBy("mid").agg(F.avg("score").alias("avg_score"))
    # df4.show()
    print("3 查询大于平均分的电影的数量")
    spark.sql("""
                SELECT COUNT(*) AS count FROM
                (SELECT a.mid,a.score,average_score
                FROM score a
                INNER JOIN (
                    SELECT mid, avg(score) AS average_score,count(mid) AS count
                    FROM score
                    GROUP BY mid
                ) b ON a.mid = b.mid
                WHERE a.score > b.average_score);
                """).show()
    # df4 = df2.groupBy("mid").agg(F.avg("score").alias("avg_score"))
    print("4 高分电影中打分最多的用户的平均分")
    spark.sql("""
                    SELECT b.uid,format_number(avg(score),2) FROM score a
                    RIGHT JOIN
                    (SELECT uid,count FROM (
                    SELECT uid,count(score) AS count FROM score
                    WHERE score > 3
                    GROUP BY uid
                    ORDER BY count(score)
                    )
                    ORDER BY count desc LIMIT 1) b
                    ON a.uid = b.uid
                    GROUP BY b.uid;
                    """).show()

    # spark.sql("""
    #     select c.uid,c.u_count
    #     from
    #     (select uid,count(score)as u_count from score where score > 3 group by uid order by uid) as c
    #     order by c.u_count desc;
    # """).show()
    print("5 查询每个用户的平均分，最低分，最高分")
    spark.sql("""
        SELECT
            uid,
            format_number(AVG(score),2) AS average_score,
            MIN(score) AS min_score,
            MAX(score) AS max_score
        FROM
            score
        GROUP BY uid
        ORDER BY uid;
    """).show()

    print("6 查询被评分超过100次的电影的平均分，排名TOP10")
    spark.sql("""
            SELECT mid, COUNT(mid) as count,format_number(avg(score),2) as avg_score
            FROM score
            GROUP BY mid
            HAVING COUNT(mid) > 100
            ORDER BY count desc LIMIT 10;

        """).show()

