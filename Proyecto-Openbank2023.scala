// Databricks notebook source
// MAGIC %md
// MAGIC ####Importamos las librerias que necesitaremos

// COMMAND ----------

//Objetos para definir la metadata
import org.apache.spark.sql.types.{StructType, StructField}

//Podemos importar todos los utilitarios con la siguiente sentencia
import org.apache.spark.sql.types._

//Importamos todos los objetos utilitarios dentro de una variable
import org.apache.spark.sql.functions._

//Librería que crea una sesión en SPARK indicando la reserva de potencia del clúster
import org.apache.spark.sql.SparkSession

//Importamos la librería que define el tipo de dato de un Dataframe
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// MAGIC %md
// MAGIC ####Funciones de optimización

// COMMAND ----------

//PATRÓN DE DISEÑO SHOW

//Definimos una varible de control
var PARAM_SHOW_HABILITADO = true

//Definimos la función
def show(df : DataFrame) = {
  if(PARAM_SHOW_HABILITADO == true){
    df.show()
  }
}

// COMMAND ----------

//PATRÓN DE DISEÑO CHECKPOINT
def checkpoint(df : DataFrame) : DataFrame = {
  var dfCheckpoint : DataFrame = null
  
  //Generamos un nombre aleatorio para la carpeta entre 0 y 100000000
  var carpeta = "dbfs:///FileStore/tmp/" + (math.random * 100000000).toString
  
  //Guardamos el dataframe en la carpeta para liberar memoria de la cadena de procesos
  print("Aplicando checkpoint...")
  df.write.mode("overwrite").format("parquet").save(carpeta)
  df.unpersist(blocking = true)
  println(", checkpoint aplicado!")
  
  //Volvemos a leerlo
  dfCheckpoint = spark.read.format("parquet").load(carpeta)
  
  return dfCheckpoint
}

// COMMAND ----------

//PATRÓN DE DISEÑO CACHÉ

//Función para almacenar en la caché
def cache(df : DataFrame) = {
  print("Almacenando en cache...")
  df.cache()
  println(", almacenado en cache!")
}

//Función para liberar del caché un dataframe
def liberarCache(df : DataFrame) = {
  print("Liberando cache...")
  df.unpersist(blocking = true)
  println(", cache liberado!")
}

//Función para liberar todos los dataframes almacenados en la caché
def liberarTodoElCache(spark : SparkSession) = {
  print("Liberando todo el cache...")
  spark.sqlContext.clearCache()
  println(", todo el cache liberado!")
}

// COMMAND ----------

//PATRÓN DE DISEÑO REPARTITION

//Definimos el número de registros por partición
var REGISTROS_POR_PARTICION = 100000

//Función de reparticionamiento
def reparticionar(df : DataFrame) : DataFrame = {
  var dfReparticionado : DataFrame = null
  
  //Obtenemos el número de particiones actuales
  var numeroDeParticionesActuales = df.rdd.getNumPartitions
  
  //Obtenemos la cantidad de registros del dataframe
  var cantidadDeRegistros = df.count()
  
  //Obtenemos el nuevo número de particiones
  var nuevoNumeroDeParticiones = (cantidadDeRegistros / (REGISTROS_POR_PARTICION *1.0)).ceil.toInt
  
  //Reparticionamos
  print("Reparticionando a "+nuevoNumeroDeParticiones+ " particiones...")
  if(nuevoNumeroDeParticiones > numeroDeParticionesActuales){
    dfReparticionado = df.repartition(nuevoNumeroDeParticiones)
  }else{
    dfReparticionado = df.coalesce(nuevoNumeroDeParticiones)
  }
  println(", reparticionado!")
  
  return dfReparticionado
}

// COMMAND ----------

// MAGIC %md
// MAGIC ####Carga de datos

// COMMAND ----------

// MAGIC %md
// MAGIC Guardamos las rutas donde hemos cargado ambos archivos .csv

// COMMAND ----------

val fileRoute1 = "dbfs:/FileStore/proyecto_final/world_happiness_report_2021.csv"
val fileRoute2 = "dbfs:/FileStore/proyecto_final/world_happiness_report.csv"

// COMMAND ----------

// MAGIC %md
// MAGIC Cargamos los archivos .csv a dos DataFrame
// MAGIC - Indicamos que el archivo contiene los headers
// MAGIC - Inferimos el tipo de datos de cada columna

// COMMAND ----------

val df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(fileRoute1)
val df2 = spark.read.option("header", "true").option("inferSchema", "true").csv(fileRoute2)

// COMMAND ----------

// MAGIC %md
// MAGIC Mostramos el primer DataFrame para ver su contenido

// COMMAND ----------

display(df1)

// COMMAND ----------

// MAGIC %md
// MAGIC Mostramos su esquema para ver la estructura del DataFrame

// COMMAND ----------

df1.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Mostramos el segundo DataFrame para ver su contenido

// COMMAND ----------

display(df2)

// COMMAND ----------

// MAGIC %md
// MAGIC Mostramos su esquema para ver la estructura del DataFrame

// COMMAND ----------

df2.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ####Limpeza de datos

// COMMAND ----------

// MAGIC %md
// MAGIC TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ####Consultas

// COMMAND ----------

// MAGIC %md
// MAGIC #####1. ¿Cuál es el país más “feliz” del 2021 según la data? (considerar la columna “Ladder score”)

// COMMAND ----------

val res1 = df1.orderBy(col("Ladder score").desc).limit(1)
display(res1)

// COMMAND ----------

// MAGIC %md
// MAGIC #####2. ¿Cuál es el país más “feliz” del 2021 por continente según la data?

// COMMAND ----------

// MAGIC %md
// MAGIC Añadimos la columna "Continent", donde agrupamos los países por continentes teniendo en cuenta su "Regional indicator"

// COMMAND ----------

val dfContinent = df1.withColumn("Continent", when(col("Country name") === "Australia" || col("Country name") === "New Zealand", "Oceania")
                                .when(col("Regional indicator").contains("Europe"), "Europe")
                                .when(col("Regional indicator").contains("Asia"), "Asia")
                                .when(col("Regional indicator").contains("America"), "America")
                                .when(col("Regional indicator").contains("Africa"), "Africa")
                                .otherwise("Other"))

display(dfContinent)

// COMMAND ----------

val res2 = dfContinent.groupBy("Continent").agg(
  first("Country name").alias("Country name"),
  max("Ladder score").alias("Ladder score")
)

display(res2)

// COMMAND ----------

// MAGIC %md
// MAGIC #####3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

// COMMAND ----------

// MAGIC %md
// MAGIC En primer lugar, añadimos la columna "year" a los países del 2021 y renombramos la columna "Ladder score" a "Life Ladder" para que sea igual en ambos dataframes

// COMMAND ----------

val df1YearLadder = df1.withColumn("year", lit(2021)).withColumnRenamed("Ladder score", "Life Ladder")
display(df1YearLadder)

// COMMAND ----------

// MAGIC %md
// MAGIC Seleccionamos solo las columnas que nos interesan

// COMMAND ----------

val df1New = df1YearLadder.select("Country name", "Life Ladder", "year")

// COMMAND ----------

// MAGIC %md
// MAGIC Nos quedamos con las 3 mismas columnas del otro ranking

// COMMAND ----------

val df2New = df2.select("Country name", "Life Ladder", "year")

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora, unimos en un solo dataframe ambos rankings de años previos y de 2021

// COMMAND ----------

val dfUnion = df1New.union(df2New)
display(dfUnion)

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos una partition para poder hacer la consulta

// COMMAND ----------

val partByYear = Window.partitionBy(col("year")).orderBy(col("Life Ladder").desc)
val res3 = dfUnion.withColumn("Ranking", rank().over(partByYear)).filter(col("Ranking") === 1).groupBy("Country name").count().orderBy(col("count").desc)
display(res3)

// COMMAND ----------

// MAGIC %md
// MAGIC #####4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?

// COMMAND ----------

display(df2)

// COMMAND ----------

val partition = Window.partitionBy(col("year")).orderBy(col("Life Ladder").desc)
val res4 = df2.withColumn("Ranking", rank().over(partByYear)).filter(col("year")===2020).orderBy(col("Log GDP per capita").desc).limit(1).select(col("Country name"),col("Ranking"))

display(res4)

// COMMAND ----------

// MAGIC %md
// MAGIC #####5. ¿En qué porcentaje ha variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?

// COMMAND ----------

// MAGIC %md
// MAGIC Obtenemos el GDP promedio de 2020

// COMMAND ----------

val avgGDP2020 = df2.filter(col("year")===2020).select(avg(col("Log GDP per capita"))).head().getDouble(0)
println(avgGDP2020)

// COMMAND ----------

// MAGIC %md
// MAGIC Obtenemos el GDP promedio de 2021

// COMMAND ----------

val avgGDP2021 = df1.select(avg(col("Logged GDP per capita"))).head().getDouble(0)
println(avgGDP2021)

// COMMAND ----------

// MAGIC %md
// MAGIC Calculamos el porcentaje de variación entre los valores

// COMMAND ----------

val gdpVariation = ((avgGDP2021 - avgGDP2020) / avgGDP2020) * 100
println("El GDP promedio ha variado un " + gdpVariation + "%, por lo que ha disminuido")

// COMMAND ----------

// MAGIC %md
// MAGIC #####6. ¿Cuál es el país con mayor expectativa de vida (“Healthy life expectancy at birth”)? Y ¿Cuánto tenía en ese indicador en el 2019?

// COMMAND ----------

val df1New = df1.withColumn("year", lit(2021)).select("Country name", "Healthy life expectancy", "year")
display(df1New)

// COMMAND ----------

val df2New = df2.withColumnRenamed("Healthy life expectancy at birth", "Healthy life expectancy").select("Country name", "Healthy life expectancy", "year")
display(df2New)

// COMMAND ----------

val dfLife = df1New.union(df2New)
display(dfLife)

// COMMAND ----------

val res6 = dfLife.groupBy("Country name").agg(
  avg("Healthy life expectancy").as("Avg Healthy life expectancy")
  ).orderBy(col("Avg Healthy life expectancy").desc)
display(res6)

// COMMAND ----------

val res7 = dfLife.filter(col("year")===2019).orderBy(col("Healthy life expectancy").desc)
display(res7)
