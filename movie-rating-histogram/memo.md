# Memo from the Course

## Spark3 
- GPU Instance Support
- Deeper Kubernetes Support
- Binary File Support 
- SparkGraph - Cypher Query
- Delta Lakes (Data Lakes) - ACID

### Spark - an Engine for large-scale data processing
- scalable
    - Driver Program: Spark Context
    - cluster Manager: Spark, YARN (Yet Another Resource Negotiator - Hadoop)
    - Executors (Cache, Tasks) 

- Fast
    - DAG Engine (Direct Acyclic Graph) - optimizes workflows
    - 100x faster than Hadoop MapReduce in memory
    - 10x faster on disk

- Use Cases
    - Amazon
    - Ebay
    - NASA JPL: Deep Space Network
    - Groupon
    - TripAdvisor
    - Yahoo

- Code
    - Python / Java / Scala (native to Spark)
    - RDD: an Object used in Spark
    - Resilient Distributed Dataset

- Components
    - Spark Streaming (real-time)
        e.g. web log
    - Spark SQL (structured data)
        e.g. data warehouse
    - MLLib (machine learning)
    - GraphX (social graph)

## RDD - Resilient Distributed Dataset - core object (base)
    - RDD -> DataFrame -> DataSet
    - primary uer-facing API
    - immutable distributed collection of elements of data
    - partitioned across nodes in cluster
    - operated in parallel with low-level API
    

### RDD - types of operations
    * The most expensive operations - require communication between nodes
    - Transformation: RDD -> RDD
        - map / filter / sample 
        - No communication
    - Action: RDD -> Python-object
        - reduce / collect / count / take
        - Some communication
    - Shuffle: RDD -> RDD
        - shuffle needed
        - repartition / sortByKey / reduceByKey / join
        - A LOT of communication

### RDD Transformation
    - map  
    - flatmap
    - filter 
    - distinct - unique value
    - sample - smaller set
    - union / intersection / subtract / cartesian

    e.g. 
    rdd = sc.parallelize([1, 2, 3 4])
    rdd.map(lambda x: x*x) - input function
    def func(x):
        return x*x
    rdd.map(func(x))

    output 
    [1, 4, 9, 16]

### RDD Actions
    - collect
    - count
    - countByValue
    - take
    - top
    - reduce - aggregation

## LAZY Evaluation 
    - nothing happens until an action is called


# Data Flow - PySpark Internal
- Build on top of Spark's Java API.
- Data is processed in Python / Cached / Shuffled in JVM
- SparkContext - uses Py4J to launch a JVM and create a JavaSparkContext on driver for local communication between Python and Java SparkContext Objects;
- RDD transformation in Python mapped to transformations on PythonRDD objects in Java


