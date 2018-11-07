# Spark-sql与Spark-core的结合

​                                                                                                                         刘凯 2018-11-07

 2.0开始DataFrame已经成为DataSet[Row]的一个Type。

 以DataSet的show方法的源码来一步步解析spark-sql是如何与spark-core相结合的：

​     DataSet.show()=>DataSet.show(20)=>DataSet.showString (numRows, truncate = 20)

=>head(20)=> withAction("head", limit(n).queryExecution)(collectFromPlan)

SparkSql由Sql逻辑转为Rdd的代码是在SparkPlan的doExecute方法实现的。Show()方法最终的返回值为Array[InternalRow]。而Rdd的执行结果是如何放入Array[InternalRow]请看第4个方法。

而Spark-sql的核心逻辑则在QueryExecutiron。该类描述了一个sql如何成为物理执行计划的整个流程。

## 核心方法代码：

## 1 DataSet.withAction方法

// Wrap a Dataset action to track the QueryExecution and time cost, then report to the

 // user-registered callback functions.

该方法最后的返回值为Array[InternalRow]请看第4步。

```
/**
   * Wrap a Dataset action to track the QueryExecution and time cost, then report to the
   * user-registered callback functions.
   */
  private def withAction[U](name: String, qe: QueryExecution)(action: SparkPlan => U) = {
    try {
      qe.executedPlan.foreach { plan =>
        plan.resetMetrics()
      }
      val start = System.nanoTime()
      val result = SQLExecution.withNewExecutionId(sparkSession, qe) {
        action(qe.executedPlan)
      }
      val end = System.nanoTime()
      sparkSession.listenerManager.onSuccess(name, qe, end - start)
      result
    } catch {
      case e: Exception =>
        sparkSession.listenerManager.onFailure(name, qe, e)
        throw e
    }
  }
```

## 
2 SQLExecution.withNewExecutionId方法：

   Wrap an action that will execute "queryExecution" to track all Spark jobs in the body so that

   we can connect them with an execution.

  

```
/**
   * Wrap an action that will execute "queryExecution" to track all Spark jobs in the body so that
   * we can connect them with an execution.
   */
  def withNewExecutionId[T](
      sparkSession: SparkSession,
      queryExecution: QueryExecution)(body: => T): T = {
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(EXECUTION_ID_KEY)
    val executionId = SQLExecution.nextExecutionId
    sc.setLocalProperty(EXECUTION_ID_KEY, executionId.toString)
    executionIdToQueryExecution.put(executionId, queryExecution)
    try {
      // sparkContext.getCallSite() would first try to pick up any call site that was previously
      // set, then fall back to Utils.getCallSite(); call Utils.getCallSite() directly on
      // streaming queries would give us call site like "run at <unknown>:0"
      val callSite = sparkSession.sparkContext.getCallSite()

      sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionStart(
        executionId, callSite.shortForm, callSite.longForm, queryExecution.toString,
        SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan), System.currentTimeMillis()))
      try {
        body
      } finally {
        sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionEnd(
          executionId, System.currentTimeMillis()))
      }
    } finally {
      executionIdToQueryExecution.remove(executionId)
      sc.setLocalProperty(EXECUTION_ID_KEY, oldExecutionId)
    }
  }
```



##  3 DataSet.collectFromPlan将executedPlan转为RDD的方法



```
/**
   * Collect all elements from a spark plan.
   */
  private def collectFromPlan(plan: SparkPlan): Array[T] = {
    // This projection writes output to a `InternalRow`, which means applying this projection is not
    // thread-safe. Here we create the projection inside this method to make `Dataset` thread-safe.
    val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
    plan.executeCollect().map { row =>
      // The row returned by SafeProjection is `SpecificInternalRow`, which ignore the data type
      // parameter of its `get` method, so it's safe to use null here.
      objProj(row).get(0, null).asInstanceOf[T]
    }
  }
```

 

## 4 Sparkplan.executeCollect()方法：

   

```
 /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[InternalRow] = {
    val byteArrayRdd = getByteArrayRdd()

    val results = ArrayBuffer[InternalRow]()
    byteArrayRdd.collect().foreach { countAndBytes =>
      decodeUnsafeRows(countAndBytes._2).foreach(results.+=)
    }
    results.toArray
  }
```



## 5 SparkPlan.getByteArrayRdd(n: Int = -1) 获得RDD[(Long, Array[Byte])]

  

```
 /**
   * Packing the UnsafeRows into byte array for faster serialization.
   * The byte arrays are in the following format:
   * [size] [bytes of UnsafeRow] [size] [bytes of UnsafeRow] ... [-1]
   *
   * UnsafeRow is highly compressible (at least 8 bytes for any column), the byte array is also
   * compressed.
   */
  private def getByteArrayRdd(n: Int = -1): RDD[(Long, Array[Byte])] = {
    execute().mapPartitionsInternal { iter =>
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 4K
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(codec.compressedOutputStream(bos))
      while (iter.hasNext && (n < 0 || count < n)) {
        val row = iter.next().asInstanceOf[UnsafeRow]
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
        count += 1
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      Iterator((count, bos.toByteArray))
    }
  }
```

 

## 6 SparkPlan.execute()方法



```
/**
   * Returns the result of this query as an RDD[InternalRow] by delegating to `doExecute` after
   * preparations.
   *
   * Concrete implementations of SparkPlan should override `doExecute`.
   */
  final def execute(): RDD[InternalRow] = executeQuery {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("A canonicalized plan is not supposed to be executed.")
    }
    doExecute()
  }
```



## 7 executeQuery

   对SparkPlan的DoExecute()的包装

```
 /**
   * Executes a query after preparing the query and adding query plan information to created RDDs
   * for visualization.
   */
  protected final def executeQuery[T](query: => T): T = {
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()
      waitForSubqueries()
      query
    }
  }
```



##  8 SparkPaln.doExecute(): RDD[InternalRow]方法

该方法是核心方法是将SparkPlan（物理执行计划）转为Rdd的核心方法

​      Produces the result of the query as an `RDD[InternalRow]`

​      Overridden by concrete implementations of SparkPlan.

   SparkPlan:物理执行计划的基类。

​     The base class for physical operators.

​     The naming convention is that physical operators end with "Exec" suffix, e.g. [[ProjectExec]].  

 ![img](file:///C:\Users\admin\AppData\Local\Temp\msohtmlclip1\01\clip_image002.jpg)

 我们看一个最简单的doExecute()方法,CoalesceExec的代码

```
case class CoalesceExec(numPartitions: Int, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else UnknownPartitioning(numPartitions)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    if (numPartitions == 1 && child.execute().getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new CoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      child.execute().coalesce(numPartitions, shuffle = false)
    }
  }
}
```



 

 

 

 

 

 

 

 

 

 

 

 

 

 