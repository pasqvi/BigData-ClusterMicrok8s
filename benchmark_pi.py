#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Benchmark Spark: Stima di \CF\80 (Monte Carlo)
-----------------------------------------
Esegue un calcolo CPU-bound per misurare la differenza di tempo
tra 1 e pi\C3\B9 worker Spark nel cluster Kubernetes.
"""

import argparse
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, ArrayType
)


def detect_num_executors(sc):
    """
    Rileva il numero di executor attivi (escludendo il driver)
    e i relativi endpoint.
    Strategia:
      1) statusTracker.getExecutorInfos()  (preferita)
      2) fallback: getExecutorMemoryStatus() con conversione Scala\E2\86\92Java
    """
    try:
        jsc = sc._jsc
        tracker = jsc.sc().statusTracker()
        infos = tracker.getExecutorInfos()  # Scala Array[ExecutorInfo]
        endpoints = []
        # Scala Array supporta .length() e .apply(i)
        for i in range(infos.length()):
            info = infos.apply(i)
            eid = info.executorId()
            if eid != "driver":
                endpoints.append(f"{eid}@{info.hostPort()}")
        return len(endpoints), endpoints
    except Exception:
        # Fallback: usa la mappa della memoria degli executor
        try:
            jsc = sc._jsc
            jvm = sc._jvm
            scala_map = jsc.sc().getExecutorMemoryStatus()  # scala.collection.Map
            # Converte Scala Map -> Java Map
            jmap = jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(scala_map).asJava()
            endpoints = []
            it = jmap.keySet().iterator()
            while it.hasNext():
                bm = it.next()  # org.apache.spark.storage.BlockManagerId
                s = str(bm)     # es: BlockManagerId(0, 10.1.245.14, 7079, None)
                if "driver" in s.lower():
                    continue
                endpoints.append(s)
            return len(endpoints), endpoints
        except Exception as e2:
            print(f"\E2\9A\A0\EF\B8\8F  Impossibile determinare gli executor attivi: {e2}")
            return 0, []


def run_benchmark(spark, samples_per_partition, num_partitions, seed):
    """
    Stima \CF\80 tramite metodo Monte Carlo.
    Ogni partizione genera `samples_per_partition` punti casuali.
    """
    sc = spark.sparkContext
    rdd = sc.parallelize(range(num_partitions), num_partitions)

    def sampler(pid, iterator):
        import random
        r = random.Random(seed + pid * 1337)
        inside = 0
        for _ in range(samples_per_partition):
            x, y = r.random(), r.random()
            if x * x + y * y <= 1.0:
                inside += 1
        yield inside

    t0 = time.perf_counter()
    total_inside = rdd.mapPartitionsWithIndex(sampler).sum()
    elapsed = time.perf_counter() - t0
    pi_est = 4.0 * total_inside / (samples_per_partition * num_partitions)
    return pi_est, elapsed


def main():
    parser = argparse.ArgumentParser(description="Benchmark Spark Monte Carlo \CF\80")
    parser.add_argument("--samples-per-partition", type=int, default=250_000,
                        help="Numero di punti per partizione (default: 250k)")
    parser.add_argument("--partitions", type=int, default=0,
                        help="Numero di partizioni (default: 4 * parallelismo)")
    parser.add_argument("--seed", type=int, default=42, help="Seed per RNG")
    parser.add_argument("--output", type=str, default=None,
                        help="Percorso S3A/MinIO per salvare i risultati (es. s3a://test-bucket/benchmarks/bench_pi)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("benchmark-pi").getOrCreate()
    sc = spark.sparkContext

    default_par = sc.defaultParallelism
    num_partitions = args.partitions if args.partitions > 0 else max(1, default_par * 4)

    print(f"\n\E2\9A\99\EF\B8\8F  Avvio benchmark con {num_partitions} partizioni, "
          f"{args.samples_per_partition:,} campioni per partizione")

    num_exec, endpoints = detect_num_executors(sc)
    pi, elapsed = run_benchmark(spark, args.samples_per_partition, num_partitions, args.seed)

    result = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "master": sc.master,
        "appId": sc.applicationId,
        "defaultParallelism": default_par,
        "numPartitions": num_partitions,
        "samplesPerPartition": args.samples_per_partition,
        "numExecutors": num_exec,
        "executorEndpoints": endpoints,
        "piEstimate": pi,
        "elapsedSeconds": elapsed
    }

    print("\n=== RISULTATI BENCHMARK ===")
    print(json.dumps(result, indent=2))

    if args.output:
        try:
            schema = StructType([
                StructField("timestamp", StringType(), True),
                StructField("master", StringType(), True),
                StructField("appId", StringType(), True),
                StructField("defaultParallelism", IntegerType(), True),
                StructField("numPartitions", IntegerType(), True),
                StructField("samplesPerPartition", IntegerType(), True),
                StructField("numExecutors", IntegerType(), True),
                StructField("executorEndpoints", ArrayType(StringType()), True),
                StructField("piEstimate", DoubleType(), True),
                StructField("elapsedSeconds", DoubleType(), True)
            ])

            df = spark.createDataFrame([result], schema=schema)
            df.coalesce(1).write.mode("append").json(args.output)
            print(f"\n\E2\9C\85 Risultato salvato su: {args.output}")

        except Exception as e:
            print(f"\E2\9A\A0\EF\B8\8F  Errore nel salvataggio su MinIO/S3A (fallback RDD): {e}")
            try:
                sc.parallelize([json.dumps(result)]).saveAsTextFile(args.output)
                print(f"\E2\9C\85 Salvato con metodo alternativo su: {args.output}")
            except Exception as e2:
                print(f"\E2\9D\8C Salvataggio fallito anche in fallback: {e2}")

    spark.stop()


if __name__ == "__main__":
    main()
