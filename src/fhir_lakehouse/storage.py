import os
import tempfile
import zipfile

from pyspark.sql import SparkSession


def get_spark(app_name="fhir-lakehouse"):
    """Use the session the notebook already has (Fabric / Databricks).

    Outside a notebook build a local one with Delta - needs `pip install delta-spark`,
    the jars are pulled from maven the first time.
    """
    spark = SparkSession.getActiveSession()
    if spark is not None:
        return spark

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


_shipped = set()


def ship_package_to_executors(spark):
    """Python UDFs run on the executors, which don't see the sys.path entry the
    notebook adds on the driver. Zip the package and hand it to Spark once per process.
    Not needed if the package is installed as a wheel in a Fabric environment."""
    import fhir_lakehouse

    pkg_dir = os.path.dirname(os.path.abspath(fhir_lakehouse.__file__))
    if pkg_dir in _shipped:
        return
    zip_path = os.path.join(tempfile.mkdtemp(prefix="fhir_pkg_"), "fhir_lakehouse.zip")
    with zipfile.ZipFile(zip_path, "w") as zf:
        for folder, _, files in os.walk(pkg_dir):
            for f in files:
                if f.endswith(".py"):
                    full = os.path.join(folder, f)
                    zf.write(full, os.path.relpath(full, os.path.dirname(pkg_dir)))
    spark.sparkContext.addPyFile(zip_path)
    _shipped.add(pkg_dir)


class Lakehouse:
    """Paths + Delta read/write for one lakehouse.

    files_root / tables_root are what Spark uses ("Files", "Tables" with a default
    lakehouse attached, or full abfss paths). files_mount is the same Files folder as
    seen by plain python (/lakehouse/default/Files in Fabric); empty means write
    through notebookutils instead.
    """

    def __init__(self, spark, cfg):
        lh = cfg["lakehouse"]
        self.spark = spark
        self.files_root = lh["files_root"].rstrip("/")
        self.files_mount = (lh.get("files_mount") or "").rstrip("/")
        self.tables_root = lh["tables_root"].rstrip("/")
        self.raw_folder = lh.get("raw_folder", "raw/fhir").strip("/")

    def table_path(self, name):
        return f"{self.tables_root}/{name}"

    def file_path(self, rel_path):
        return f"{self.files_root}/{rel_path}"

    def write_file(self, rel_path, text):
        """Write a text file under Files/ and return the path Spark should use to read it."""
        if self.files_mount:
            target = os.path.join(self.files_mount, rel_path)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "w", encoding="utf-8") as fh:
                fh.write(text)
        else:
            import notebookutils  # only exists inside Fabric

            notebookutils.fs.put(self.file_path(rel_path), text, True)
        return self.file_path(rel_path)

    # delta helpers

    def exists(self, name):
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(self.spark, self.table_path(name))

    def read(self, name):
        return self.spark.read.format("delta").load(self.table_path(name))

    def append(self, df, name):
        df.write.format("delta").mode("append").save(self.table_path(name))

    def overwrite(self, df, name):
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true")
           .save(self.table_path(name)))

    def upsert(self, df, name, keys):
        if not self.exists(name):
            self.append(df, name)
            return
        from delta.tables import DeltaTable

        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (DeltaTable.forPath(self.spark, self.table_path(name)).alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
