// Databricks notebook source
// MAGIC %md
// MAGIC ## Paso 1. Importar librerías necesarias
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.expressions._


// COMMAND ----------

// MAGIC %md
// MAGIC ## Paso 2. Definición de funciones de optimización

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función SHOW

// COMMAND ----------

var PARAM_SHOW = true

def show(df : DataFrame) = {
  if(PARAM_SHOW == true) {
    df.show()
  }
}


// COMMAND ----------

// MAGIC %md
// MAGIC #### Función CHECKPOINT

// COMMAND ----------

def checkpoint(df : DataFrame) : DataFrame = {
  var dfCheckpoint : DataFrame = null

  var carpeta = "dbfs:///FileStore/trabajo_final/tmp" + (math.random * 100000000).toString

  println("Se está creando un checkpoint.")
  df.write.mode("overwrite").format("parquet").save(carpeta)
  df.unpersist(blocking = true)
  println("Se ha aplicado el checkpoint.")

  dfCheckpoint = spark.read.format("parquet").load(carpeta)

  return dfCheckpoint
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Funciones CACHÉ

// COMMAND ----------

def cache(df : DataFrame) = {
  println("Se va a almacenar el DataFrame en caché.")
  df.cache()
  println("Se ha almacenado el DataFrame en caché.")
}

def liberarCache(df : DataFrame) = {
  println("Se va a liberar la caché.")
  df.unpersist(blocking = true)
  println("Se ha liberado la caché.")
}

def liberarTodoCache(spark : SparkSession) = {
  println("Se va a liberar toda la caché.")
  spark.sqlContext.clearCache()
  println("Toda la caché ha sido liberada.")
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función REPARTITION

// COMMAND ----------

var registrosPorParticion = 100000

def reparticionar(df : DataFrame) : DataFrame = {
  var dfReparticionado : DataFrame = null

  var particionesActuales = df.rdd.getNumPartitions

  var cantidadRegistros = df.count()

  var nuevasParticiones = (cantidadRegistros / (registrosPorParticion * 1.0)).ceil.toInt

  println("Se va a reparticionar el DataFrame a " + nuevasParticiones + " particiones.")

  if(nuevasParticiones > particionesActuales){
    dfReparticionado = df.repartition(nuevasParticiones)
  } else {
    dfReparticionado = df.coalesce(nuevasParticiones)
  }

  println("Se ha reparticionado correctamente a " + dfReparticionado.rdd.getNumPartitions + " particiones.")

  return dfReparticionado
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Reserva de potencia del clúster

// COMMAND ----------

var spark = SparkSession.builder.
appName("Nombre de mi aplicacion").
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
// MAGIC ## Paso 3. Lectura de datos

// COMMAND ----------

var dfReport2021 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/FileStore/trabajo_final/data/world_happiness_report_2021.csv")

display(dfReport2021)

// COMMAND ----------

dfReport2021.printSchema

// COMMAND ----------

var dfReportTotal = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/FileStore/trabajo_final/data/world_happiness_report.csv")

display(dfReportTotal)

// COMMAND ----------

dfReportTotal.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC #### Exploratory Data Analysis (EDA)

// COMMAND ----------

// Eliminamos las columnas que no coinciden y creamos una con valor 2021
var dfReport2021NewColumns = dfReport2021
.withColumn("Year", lit(2021).cast("Integer"))
.drop(
  "upperwhisker","lowerwhisker","Standard error of ladder score","Ladder score in Dystopia","Explained by: Log GDP per capita","Explained by: Social support","Explained by: Healthy life expectancy","Explained by: Freedom to make life choices","Explained by: Generosity","Explained by: Perceptions of corruption","Dystopia + residual","Social support","Freedom to make life choices","Generosity","Perceptions of corruption"
  )

// Cambiamos el nombre de las columnas
var dfReportTotalNewColumns = dfReportTotal
.withColumnRenamed("year","Year")
.withColumnRenamed("Life Ladder", "Ladder score")
.withColumnRenamed("Log GDP per capita", "Logged GDP per capita")
.withColumnRenamed("Healthy life expectancy at birth", "Healthy life expectancy")
.drop("Positive affect","Negative affect","Social support","Freedom to make life choices","Generosity","Perceptions of corruption")

// COMMAND ----------

display(dfReport2021NewColumns.describe())

// COMMAND ----------

display(dfReportTotalNewColumns.describe())

// COMMAND ----------

// MAGIC %md
// MAGIC #### Repartition y cache

// COMMAND ----------

// MAGIC %md
// MAGIC Reparticionar los DF

// COMMAND ----------

dfReport2021NewColumns = reparticionar(dfReport2021NewColumns)
dfReportTotalNewColumns = reparticionar(dfReportTotalNewColumns)

// COMMAND ----------

// MAGIC %md
// MAGIC Almacenar en la caché

// COMMAND ----------

cache(dfReport2021NewColumns)
cache(dfReportTotalNewColumns)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Paso 4. Actividades propuestas

// COMMAND ----------

// MAGIC %md
// MAGIC #### Ejercicio 1. ¿Cuál es el país más “feliz” del 2021 según la data?

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos una variable que sea el DF de 2021 con un select de las columnas que nos interesan limitando a un resultado el output

// COMMAND ----------

val dfReporte1 = dfReport2021NewColumns.select(
  col("Country name"),
  col("Ladder score")
).orderBy(col("Ladder score").desc).limit(1)

display(dfReporte1)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Ejercicio 2. ¿Cuál es el país más “feliz” del 2021 por continente según la data?

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos un nuevo DataFrame en el que incluimos la columna continentes basándonos en la columna Indicador Regional. En el caso de "North America and ANZ" hay que tener en cuenta que pertenecen a diferentes continentes.

// COMMAND ----------

def setContinent(region: String, country: String): String = {
  country match {
    case "New Zealand" | "Australia" => return "Oceania"
    case _ => 
  }

  region match {
    case a if region contains "Asia" => return "Asia"
    case b if region contains "Europe" => return "Europe"
    case c if region contains "Africa" => return "Africa"
    case d if region contains "America" => return "America"
    case _ => return "Other"
  }
}


val udfSetContinent = udf((region: String,country: String) => setContinent(region,country))

val dfReport2021Continents = dfReport2021NewColumns.withColumn("Continent", udfSetContinent(
    col("Regional indicator"),
    col("Country name")
  )
)

display(dfReport2021Continents)

// COMMAND ----------

// MAGIC %md
// MAGIC A partir del DataFrame anterior, agrupamos por continente y le aplicamos la función de agregado MAX, para después ordenar por esta función y obtener el país más feliz de 2021 por continente.

// COMMAND ----------

var dfReporte2 = dfReport2021Continents.groupBy("Continent").agg(first("Country name").as("Country"),max("Ladder score").as("Max ladder")).orderBy(desc("Max ladder"))

display(dfReporte2)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Ejercicio 3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

// COMMAND ----------

// MAGIC %md
// MAGIC Creo un nuevo DataFrame uniendo los dos DF que tengo, luego creo una ventana con una partición por año y ordenada por el Ladder Score.
// MAGIC
// MAGIC Después, aplico los cambions necesarios al DataFrame Unido, incluyendo la ventana, para obtener el país que más veces ocupó el primer lugar en todos los años.

// COMMAND ----------

var dfUnido = dfReport2021NewColumns.select(
  $"Country name",
  $"Ladder score",
  $"Year"
  ).union(dfReportTotalNewColumns.select(
    $"Country name",
    $"Ladder score",
    $"Year"
    )
  )

cache(dfUnido)

val windowSpec  = Window.partitionBy("Year").orderBy(desc("Ladder score"))

var dfReporte3 = dfUnido
  .withColumn("Ranking",rank().over(windowSpec))
  .filter($"Ranking" === 1)
  .drop($"Ranking").orderBy(desc("Year"))
  .groupBy("Country Name").agg(count("Year").as("Times#1"))
  .orderBy(desc("Times#1"))


display(dfReporte3)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Ejercicio 4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?

// COMMAND ----------

// MAGIC %md
// MAGIC Creo dos ventanas: una para el ranking de GDP y otra para el ranking del Ladder Score.
// MAGIC
// MAGIC Esto se hace para poder filtrar por el que tiene mayor GDP y para que aparezca el puesto respecto al Ladder Score del país cuyo GDP es el primero.

// COMMAND ----------

val windowSpecGDP  = Window.partitionBy("Year").orderBy(desc("Logged GDP per capita"))
val windowSpecLadder  = Window.partitionBy("Year").orderBy(desc("Ladder score"))


val dfReporte4Columns = dfReportTotalNewColumns.select(
  $"Country name",
  $"Logged GDP per capita",
  $"Ladder score",
  $"Year"
  )


cache(dfReporte4Columns)

var dfReporte4 = dfReporte4Columns
  .withColumn("RankingGDP",rank().over(windowSpecGDP))
  .withColumn("RankingLadder",rank().over(windowSpecLadder))
  .filter(($"Year" === 2020) && ($"RankingGDP" === 1))
  .drop($"RankingGDP",$"Year")

display(dfReporte4)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Ejercicio 5. ¿En qué porcentaje ha variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?

// COMMAND ----------

var dfAvg2021 = dfReport2021NewColumns
.groupBy($"Year").agg(avg($"Logged GDP per capita").as("Avg_2021"))

var dfAvg2020 = dfReportTotalNewColumns
.groupBy($"Year").agg(avg($"Logged GDP per capita").as("Avg_2020"))
.filter(($"Year") === 2020)

cache(dfAvg2021)
cache(dfAvg2020)

val avg2021 = dfAvg2021.first.getDouble(1)
val avg2020 = dfAvg2020.first.getDouble(1)

val percentage2021 = ((avg2021-avg2020)/avg2020)*100

println("----------------------------------------------------------------------------")
println("El porcentaje de variación del promedio de GDP ha sido de un " +percentage2021+ "%")
println("----------------------------------------------------------------------------")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Ejercicio 6. ¿Cuál es el país con mayor expectativa de vida (“Healthy life expectancy at birth”)? Y ¿cuánto tenía en ese indicador en el 2019?

// COMMAND ----------

var dfUnido = dfReport2021NewColumns.select(
  $"Country name",
  $"Healthy life expectancy",
  $"Year"
  ).union(dfReportTotalNewColumns.select(
    $"Country name",
    $"Healthy life expectancy",
    $"Year"
  )
)

cache(dfUnido)

var dfUnidoFilter = dfUnido
  .filter(($"Year" === 2020) || ($"Year" === 2019) || ($"Year" === 2018) || ($"Year" === 2017))

cache(dfUnidoFilter)

var dfReporte6 = dfUnidoFilter
  .groupBy($"Country name")
    .agg(avg($"Healthy life expectancy").as("Avg life expectancy"))
    .orderBy(desc("Avg life expectancy")).limit(1)

display(dfReporte6)

// COMMAND ----------

var valueLiveExpectancy = dfUnidoFilter
.filter(($"Year" === 2019) && 
  ($"Country name".like(dfUnidoFilter.groupBy($"Country name")
    .agg(avg($"Healthy life expectancy").as("Avg life expectancy"))
    .orderBy(desc("Avg life expectancy")).first.getString(0))))

display(valueLiveExpectancy)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Paso 5. Guardado de datos

// COMMAND ----------

dfReporte1.write.format("parquet").mode("overwrite").save("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_1")

dfReporte2.write.format("parquet").mode("overwrite").save("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_2")

dfReporte3.write.format("parquet").mode("overwrite").save("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_3")

dfReporte4.write.format("parquet").mode("overwrite").save("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_4")

dfReporte6.write.format("parquet").mode("overwrite").save("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_6_1")

valueLiveExpectancy.write.format("parquet").mode("overwrite").save("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_6_2")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Comprobación de carga de datos

// COMMAND ----------

val readCheck1 = spark.read.parquet("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_1")
println("Reporte 1")
readCheck1.show(false)

val readCheck2 = spark.read.parquet("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_2")
println("Reporte 2")
readCheck2.show(false)

val readCheck3 = spark.read.parquet("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_3")
println("Reporte 3")
readCheck3.show(false)

val readCheck4 = spark.read.parquet("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_4")
println("Reporte 4")
readCheck4.show(false)

val readCheck6_1 = spark.read.parquet("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_6_1")
println("Reporte 6.1")
readCheck6_1.show(false)

val readCheck6_2 = spark.read.parquet("dbfs:///FileStore/_spark/output/trabajo_final/Reporte_6_2")
println("Reporte 6.2")
readCheck6_2.show(false)

// COMMAND ----------


