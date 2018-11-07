# SparkSQL – 从0到1认识Catalyst



  http://hbasefly.com/2017/03/01/sparksql-catalyst/  看到讲的最明白的文章

   

  Sparksql代码实在过于庞大梳理起来还是蛮麻烦的。先将草稿存着，后续有时间了再看吧，大致流程已经理解啦。

   以es-hadoop中的elasticsearch-spark-20_2.11包为例。解析sparksql是如何实现用sql语义来读取操作存储在elsaticsearch中的索引数据的原理。

​     由上面链接的文章我们已经知晓，sql语义在解析的时候会有一个parser阶段。该阶段将会把我们sql语句解析成为语法树。

​     而sparksql要读取elasticsearch中的索引数据第一步就是创建临时表，并指定DataSource的来源以及类型等信息。如代码：

​    

```
sqlContext.sql(
   "CREATE TEMPORARY TABLE myPeople    " + 
   "USING org.elasticsearch.spark.sql " + 
   "OPTIONS ( resource 'spark/people', nodes 'localhost:9200')" ) 
```

 

| Name     | defaultValue             | Description                                                  |
| -------- | ------------------------ | ------------------------------------------------------------ |
| path     | Elasticsearch index/type | required                                                     |
| pushdown | true                     | Whether to translate (push-down) Spark SQL into Elasticsearch |
| strict   | false                    | Whether to use exact (not analyzed) matching or not (analyzed) |

1  usering clause 定义了DataSource 的provider。在elasticsearch-spark中的RelationProvider的实现类为org.elasticsearch.spark.sql.DefaultSource.

sparksql中RelationProvider的解释如下：

```
/**
 * Implemented by objects that produce relations for a specific kind of data source.  When
 * Spark SQL is given a DDL operation with a USING clause specified (to specify the implemented
 * RelationProvider), this interface is used to pass in the parameters specified by a user.
 *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class will be instantiated each time a DDL call is made.
```

2 elasticsearch-spark [configuration options](https://www.elastic.co/guide/en/elasticsearch/hadoop/master/configuration.html), 强制性参数 `resource`. 为了方便可以es前缀或者跳过他。

   关于options更清楚的描述请看：https://github.com/elastic/elasticsearch-hadoop/issues/1082.

DataSourceStrategy定义翻译策略，apply方法会发现模式匹配到的规则。



​     select()是翻译为逻辑计划之后，将会通过模式匹配PhysicalOperation（patterns）,filter也是一样的。最终生产物理计划。对应的物理计划是一个RDD.实际上到物理计划这一段就已经完成了RDDGraph的形成。RDDGraph是一个逻辑概念哈，并不存在这样的类哈。在UI包中存在RDDOperationGraph对应类似的概念。

代码如下：

```
def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: CatalystScan, _, _, _)) =>
      pruneFilterProjectRaw(
        l,
        projects,
        filters,
        (requestedColumns, allPredicates, _) =>
          toCatalystRDD(l, requestedColumns, t.buildScan(requestedColumns, allPredicates))) :: Nil

    case PhysicalOperation(projects, filters,
                           l @ LogicalRelation(t: PrunedFilteredScan, _, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: PrunedScan, _, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, _) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray))) :: Nil

    case l @ LogicalRelation(baseRelation: TableScan, _, _, _) =>
      RowDataSourceScanExec(
        l.output,
        l.output.indices,
        Set.empty,
        Set.empty,
        toCatalystRDD(l, baseRelation.buildScan()),
        baseRelation,
        None) :: Nil

```

备注：

elsaticsearch-spark文档解读

https://www.elastic.co/guide/en/elasticsearch/hadoop/master/spark.html#spark-sql

options 配置

https://www.elastic.co/guide/en/elasticsearch/hadoop/master/configuration.html