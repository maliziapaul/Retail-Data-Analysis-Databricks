"""Runs Retail_Data_Analysis.py locally with stand-ins for Databricks' display() and dbutils, to check the answers."""
import os, glob, types
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate(); spark.sparkContext.setLogLevel("ERROR")
LOCAL = os.path.expanduser("~/Desktop/retail_data")
class FS:
    def ls(self, p): return [types.SimpleNamespace(name=os.path.basename(f), size=os.path.getsize(f)) for f in glob.glob(LOCAL + "/*")]
src = open("Retail_Data_Analysis.py").read().replace('"/Volumes/workspace/default/retail_data"', repr(LOCAL))
code = "\n".join(l for l in src.splitlines() if not l.startswith("# MAGIC"))
def display(df):
    df.show(df.count() if df.count() < 50 else 5, truncate=False)
exec(code, {"spark": spark, "dbutils": types.SimpleNamespace(fs=FS()), "display": display})
