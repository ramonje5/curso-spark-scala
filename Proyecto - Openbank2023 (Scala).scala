// Databricks notebook source
// MAGIC %md
// MAGIC ####Importación de librerías

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// MAGIC %md
// MAGIC ####Funciones de optimización

// COMMAND ----------

// MAGIC %md
// MAGIC Patrón de diseño **SHOW**

// COMMAND ----------

var PARAM_SHOW = true
def show(df : DataFrame) = if (PARAM_SHOW) df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Patrón de diseño **CACHÉ**

// COMMAND ----------

//Almacenar en caché
def cache(df : DataFrame) = df.cache()

//Liberar un DataFrame
def liberarCache(df : DataFrame) = df.unpersist(blocking = true)

//Liberar los DataFrames
def liberarCacheAll(spark : SparkSession) = spark.sqlContext.clearCache()

// COMMAND ----------

// MAGIC %md
// MAGIC Patrón de diseño **REPARTITION**

// COMMAND ----------

var REGISTROS_POR_PARTICION = 100000

def reparticionar(df : DataFrame) : DataFrame = {
  var dfReparticionado : DataFrame = null  
  var numeroDeParticionesActuales = df.rdd.getNumPartitions
  var cantidadDeRegistros = df.count()
  
  var nuevoNumeroDeParticiones = (cantidadDeRegistros / (REGISTROS_POR_PARTICION *1.0)).ceil.toInt
  
  print("Reparticionando a " + nuevoNumeroDeParticiones + " particiones...")
  if (nuevoNumeroDeParticiones > numeroDeParticionesActuales) dfReparticionado = df.repartition(nuevoNumeroDeParticiones) else dfReparticionado = df.coalesce(nuevoNumeroDeParticiones)
  println(", reparticionado!")
  
  return dfReparticionado
}

// COMMAND ----------

// MAGIC %md
// MAGIC ####Reserva de potencia del clúster

// COMMAND ----------

var spark = SparkSession.builder.
appName("Proyecto Openbank 2023").
config("spark.driver.memory", "1g").
config("spark.dynamicAllocation.maxExecutors", "20").
config("spark.executor.cores", "2").
config("spark.executor.memory", "5g").
config("spark.executor.memoryOverhead", "500m").
config("spark.default.parallelism", "100").
config("spark.sql.inMemoryColumnarStorage.compressed", "true").
config("spark.sql.broadcastTimeout", "300").
config("spark.sql.autoBroadcastJoinThreshold", "50000000").
getOrCreate()

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

var df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(fileRoute1)
var df2 = spark.read.option("header", "true").option("inferSchema", "true").csv(fileRoute2)

// COMMAND ----------

// MAGIC %md
// MAGIC Reparticionamos los DataFrames y los almacenamos en caché

// COMMAND ----------

df1 = reparticionar(df1)
df2 = reparticionar(df2)

cache(df1)
cache(df2)

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
// MAGIC ####Limpieza de datos

// COMMAND ----------

// MAGIC %md
// MAGIC Teniendo en cuenta las consultas requeridas en este proyecto, vamos a quedarnos con las columnas que nos interesan de cada DataFrame

// COMMAND ----------

var dfReport2021 = df1.select("Country name", "Regional indicator", "Ladder score", "Logged GDP per capita", "Healthy life expectancy")

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora, normalizaremos los nombres de las columnas para tener todos en minúsculas y sin espacios

// COMMAND ----------

dfReport2021.columns.foreach(colName => {
      val normalized = colName.toLowerCase.replace(" ", "_")
      dfReport2021 = dfReport2021.withColumnRenamed(colName, normalized)
    })

// COMMAND ----------

show(dfReport2021)

// COMMAND ----------

var dfReportTotal = df2.select("Country name", "year", "Life Ladder", "Log GDP per capita", "Healthy life expectancy at birth")

// COMMAND ----------

// MAGIC %md
// MAGIC De nuevo, normalizamos los nombres de las columnas a minúsculas y sin espacios

// COMMAND ----------

dfReportTotal.columns.foreach(colName => {
      val normalized = colName.toLowerCase.replace(" ", "_")
      dfReportTotal = dfReportTotal.withColumnRenamed(colName, normalized)
    })

// COMMAND ----------

show(dfReportTotal)

// COMMAND ----------

// MAGIC %md
// MAGIC Reparticionamos los nuevos DataFrames que usaremos para nuestras consultas y los almacenamos en caché para aumentar la velocidad de las mismas

// COMMAND ----------

dfReport2021 = reparticionar(dfReport2021)
dfReportTotal = reparticionar(dfReportTotal)

cache(dfReport2021)
cache(dfReportTotal)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Consultas

// COMMAND ----------

// MAGIC %md
// MAGIC #####1. ¿Cuál es el país más “feliz” del 2021 según la data? (considerar la columna “Ladder score”)

// COMMAND ----------

val res1 = dfReport2021.select("country_name", "ladder_score").orderBy(col("ladder_score").desc).limit(1)
show(res1)

// COMMAND ----------

// MAGIC %md
// MAGIC #####2. ¿Cuál es el país más “feliz” del 2021 por continente según los datos?

// COMMAND ----------

def setContinent(region: String, country: String): String = {
  val AFRICA_COUNTRIES = Seq("Angola","Algeria","Benin","Botswana","Burkina_faso","Burundi","Cabo_verde","Cameroon","Central_african_republic","Chad","Comoros","Democratic_republic_of_the_congo","Republic_of_the_congo","Djibouti","Egypt","Equatorial_guinea","Eritrea","Ethiopia","Gabon","Gambia","Ghana","Guinea","Guinea-bissau","Ivory_coast","Kenya","Lesotho","Liberia","Libya","Madagascar","Malawi","Mali","Mauritania","Mauritius","Morocco","Mozambique","Namibia","Niger","Nigeria","Rwanda","Sao_tome_and_principe","Senegal","Seychelles","Sierra_leone","Somalia", "South_africa", "South_sudan", "Sudan", "Swaziland", "Tanzania", "Togo", "Tunisia", "Uganda", "Zambia", "Zimbabwe")
  
  val EUROPE_COUNTRIES = Seq("Albania", "Andorra", "Austria", "Belarus", "Belgium", "Bosnia and herzegovina", "Bulgaria", "Croatia", "Cyprus", "Czech republic", "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Iceland", "Ireland", "Italy", "Kosovo", "Latvia", "Liechtenstein", "Lithuania", "Luxembourg", "Malta", "Moldova", "Monaco", "Montenegro", "Netherlands", "North macedonia", "Norway", "Poland", "Portugal", "Romania", "San marino", "Serbia", "Slovakia", "Slovenia", "Spain", "Sweden", "Switzerland",  "Ukraine", "United kingdom", "Vatican city", "Turkey")
  
  country match {
    case "Australia" | "New Zealand" => return "Oceania"
    case _ =>
  }
  region match {
    case a if (region contains "Europe") | (EUROPE_COUNTRIES.contains(country)) => return "Europe"
    case b if region contains "Asia" => return "Asia"
    case c if region contains "America" => return "America"
    case d if (region contains "Africa") & (AFRICA_COUNTRIES.contains(country)) => return "Africa"
    case e if (region contains "Africa") & !(AFRICA_COUNTRIES.contains(country)) => return "Asia"
    case f if (region contains "Commonwealth") & (EUROPE_COUNTRIES.contains(country)) => return "Europe"
    case g if (region contains "Commonwealth") & !(EUROPE_COUNTRIES.contains(country)) => return "Asia"
    case _ => return "Other"
  }
}

// Creamos la UDF asociada a la función
var udfSetContinent = udf((region: String, country: String) => setContinent(region, country))

// Registramos la UDF
spark.udf.register("udfSetContinent", udfSetContinent)

// COMMAND ----------

// MAGIC %md
// MAGIC Usando la UDF previamente definida añadimos la columna "Continent", donde agrupamos los países por continentes teniendo en cuenta su "Regional indicator"

// COMMAND ----------

val dfContinent = dfReport2021.withColumn("continent", udfSetContinent(col("regional_indicator"), col("country_name")))
show(dfContinent)

// COMMAND ----------

// MAGIC %md
// MAGIC Una vez tenemos los continentes, obtenemos el país con mayor "Ladder score" por cada uno de ellos

// COMMAND ----------

val res2 = dfContinent.groupBy("continent").agg(
  first("country_name").alias("country_name"),
  max("ladder_score").alias("ladder_score")
)

show(res2)

// COMMAND ----------

// MAGIC %md
// MAGIC #####3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

// COMMAND ----------

// MAGIC %md
// MAGIC En primer lugar, añadimos la columna "year" a los países del 2021 y renombramos la columna "Ladder score" a "Life Ladder" para que sea igual en ambos DataFrames

// COMMAND ----------

val dfReport2021YearLadder = dfReport2021.withColumn("year", lit(2021)).withColumnRenamed("ladder_score", "life_ladder")
show(dfReport2021YearLadder)

// COMMAND ----------

// MAGIC %md
// MAGIC Para este caso, seleccionamos solo las columnas que nos interesan (las mismas en ambos DataFrames)

// COMMAND ----------

val dfReport2021New = dfReport2021YearLadder.select("country_name", "life_ladder", "year")
val dfReportTotalNew = dfReportTotal.select("country_name", "life_ladder", "year")

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora, unimos en un solo DataFrame ambos rankings de años previos y de 2021

// COMMAND ----------

val dfUnion = dfReport2021New.union(dfReportTotalNew)
show(dfUnion)

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos una Window para hacer un ranking de los países por año y su "Life Ladder" y poder hacer posteriormente la consulta

// COMMAND ----------

val partByYear = Window.partitionBy(col("year")).orderBy(col("life_ladder").desc)
val res3 = dfUnion.withColumn("ranking", rank().over(partByYear)).filter(col("ranking") === 1).groupBy("country_name").count().orderBy(col("count").desc)
show(res3)

// COMMAND ----------

// MAGIC %md
// MAGIC #####4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos una Window para hacer un ranking de los países por año y su "Life Ladder" y poder hacer posteriormente la consulta

// COMMAND ----------

val partition = Window.partitionBy(col("year")).orderBy(col("life_ladder").desc)
val res4 = dfReportTotal.withColumn("ranking", rank().over(partByYear)).filter(col("year")===2020).orderBy(col("log_gdp_per_capita").desc).limit(1).select(col("country_name"),col("ranking"))
show(res4)

// COMMAND ----------

// MAGIC %md
// MAGIC #####5. ¿En qué porcentaje ha variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?

// COMMAND ----------

// MAGIC %md
// MAGIC Obtenemos el GDP promedio de 2020

// COMMAND ----------

val avgGDP2020 = dfReportTotal.filter(col("year")===2020).select(avg(col("log_gdp_per_capita"))).head().getDouble(0)
println(avgGDP2020)

// COMMAND ----------

// MAGIC %md
// MAGIC Obtenemos el GDP promedio de 2021

// COMMAND ----------

val avgGDP2021 = dfReport2021.select(avg(col("logged_gdp_per_capita"))).head().getDouble(0)
println(avgGDP2021)

// COMMAND ----------

// MAGIC %md
// MAGIC Calculamos el porcentaje de variación entre los valores

// COMMAND ----------

val res5 = ((avgGDP2021 - avgGDP2020) / avgGDP2020) * 100
println("El GDP promedio ha variado un " + res5 + "%, por lo que ha disminuido de 2020 a 2021")

// COMMAND ----------

// MAGIC %md
// MAGIC #####6. ¿Cuál es el país con mayor expectativa de vida (“Healthy life expectancy at birth”)? Y ¿Cuánto tenía en ese indicador en el 2019?

// COMMAND ----------

val dfReport2021New = dfReport2021.withColumn("year", lit(2021)).select("country_name", "healthy_life_expectancy", "year")
show(dfReport2021New)

// COMMAND ----------

val dfReportTotalNew = dfReportTotal.withColumnRenamed("healthy_life_expectancy_at_birth", "healthy_life_expectancy").select("country_name", "healthy_life_expectancy", "year")
show(dfReportTotalNew)

// COMMAND ----------

val dfLife = dfReport2021New.union(dfReportTotalNew)
show(dfLife)

// COMMAND ----------

val res6 = dfLife.filter(col("year") > 2016).groupBy("country_name").agg(
  avg("healthy_life_expectancy").as("avg_healthy_life_expectancy")
  ).orderBy(col("avg_healthy_life_expectancy").desc).limit(1)
show(res6)

// COMMAND ----------

val res7 = dfLife.filter(col("year") === 2019).orderBy(col("healthy_life_expectancy").desc).limit(1)
show(res7)

// COMMAND ----------

// MAGIC %md
// MAGIC Liberamos la caché

// COMMAND ----------

liberarCacheAll(spark)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Almacenamiento de resultados

// COMMAND ----------

// MAGIC %md
// MAGIC Guardamos en formato Parquet los resultados de cada una de las consultas del proyecto en la ruta especificada

// COMMAND ----------

res1.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/proyecto-final/output/scala/consulta1")

res2.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/proyecto-final/output/scala/consulta2")

res3.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/proyecto-final/output/scala/consulta3")

res4.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/proyecto-final/output/scala/consulta4")

res6.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/proyecto-final/output/scala/consulta6")

res7.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/proyecto-final/output/scala/consulta7")

// COMMAND ----------

// MAGIC %md
// MAGIC Listamos la carpeta donde hemos guardado las respuestas para comprobar que la carga se ha completado correctamente

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/proyecto-final/output/scala"))

// COMMAND ----------

// MAGIC %md
// MAGIC Leemos las respuesta almacenadas para comprobar que se han hecho correctamente

// COMMAND ----------

// MAGIC %md
// MAGIC - Ejercicio 1:

// COMMAND ----------

var dfRes1 = spark.read.parquet("dbfs:/FileStore/proyecto-final/output/scala/consulta1")
show(dfRes1)

// COMMAND ----------

// MAGIC %md
// MAGIC - Ejercicio 2:

// COMMAND ----------

var dfRes2 = spark.read.parquet("dbfs:/FileStore/proyecto-final/output/scala/consulta2")
show(dfRes2)

// COMMAND ----------

// MAGIC %md
// MAGIC - Ejercicio 3:

// COMMAND ----------

var dfRes3 = spark.read.parquet("dbfs:/FileStore/proyecto-final/output/scala/consulta3")
show(dfRes3)

// COMMAND ----------

// MAGIC %md
// MAGIC - Ejercicio 4:

// COMMAND ----------

var dfRes4 = spark.read.parquet("dbfs:/FileStore/proyecto-final/output/scala/consulta4")
show(dfRes4)

// COMMAND ----------

// MAGIC %md
// MAGIC - Ejercicio 6.1:

// COMMAND ----------

var dfRes6 = spark.read.parquet("dbfs:/FileStore/proyecto-final/output/scala/consulta6")
show(dfRes6)

// COMMAND ----------

// MAGIC %md
// MAGIC - Ejercicio 6.2:

// COMMAND ----------

var dfRes7 = spark.read.parquet("dbfs:/FileStore/proyecto-final/output/scala/consulta7")
show(dfRes7)
