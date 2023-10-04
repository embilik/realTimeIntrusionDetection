from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when

class Model:
    def __init__(self):
        self.spark = self.create_spark_session()

    def create_spark_session(self, appName: str = "analysis") -> SparkSession:
        try:
            """
            :param appName: Spark application name, defaults to `analysis`
            :return: SparkSession
            """
            spark = SparkSession.builder.config("spark.app.name", appName).getOrCreate()
            return spark
        except Exception as e:
            # Handle any exception that occurs during SparkSession creation
            print(f"Error creating SparkSession: {str(e)}")
            return None

    from pyspark.ml.feature import StringIndexer
    from pyspark.ml import Pipeline
    from pyspark.sql import DataFrame

    def string_indexer(self,df: DataFrame) -> DataFrame:
        """
        Apply StringIndexer to the 'attack_cat' column in a DataFrame and store indexed values in a new column.

        :param df: DataFrame
        :return: DataFrame with indexed values for the 'attack_cat' column
        """
        try:
            indexer = StringIndexer(inputCol='attack_cat', outputCol='attack_cat_index').setHandleInvalid("skip")
            pipeline = Pipeline(stages=[indexer])
            index_model = pipeline.fit(df)
            indexed_df = index_model.transform(df)

            return indexed_df
        except Exception as e:
            # Handle any exception that occurs during string indexing
            print(f"Error applying StringIndexer: {str(e)}")
            return df  # Return the original DataFrame in case of an error

    def apply_string_indexers(self,df: DataFrame, cols_to_index: List[str]) -> DataFrame:
        """
        Apply StringIndexer to specified columns in a DataFrame and store indexed values in new columns.

        :param df: DataFrame
        :param cols_to_index: List of column names to be indexed
        :return: DataFrame with indexed values for the specified columns
        """
        try:
            indexers = [StringIndexer(inputCol=col_name, outputCol=catcol+"_index").setHandleInvalid("skip") for col_name in cols_to_index]
            pipeline = Pipeline(stages=indexers)
            index_model = pipeline.fit(df)
            indexed_df = index_model.transform(df)

            return indexed_df
        except Exception as e:
            # Handle any exception that occurs during string indexing
            print(f"Error applying StringIndexers: {str(e)}")
            return df  # Return the original DataFrame in case of an error

    def create_vector_assembler(df: DataFrame, input_cols: List[str], output_col: str) -> DataFrame:
        """
        Create a VectorAssembler for the specified input columns and output column.

        :param df: DataFrame
        :param input_cols: List of input column names to be assembled
        :param output_col: Output column name for the assembled vector
        :return: DataFrame with the assembled vector column
        """
        try:
            assembler = VectorAssembler(inputCols=num + [catcol + "_index" for catcol in nominal_cols],outputCol='features')
            assembler_df = assembler.transform(df)
            return assembler_df

        except Exception as e:
            # Handle any exception that occurs during VectorAssembler transformation
            print(f"Error creating VectorAssembler: {str(e)}")
            return df  # Return the original DataFrame in case of an error




