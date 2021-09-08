package exercises
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.collect_list
/** A Union dataframes
 *
 *  @constructor create a new UnionDataframe class to union dateframes
 *  @param spark the spark sesion
 */
class UnionDataframe(spark: SparkSession) {
  /** Creates a final  Dataframe reading 3 csv
   * @param comicDF dataframe comic
   * @param characterDF dataframe character
   * @param intermediateDF dataframe intermediate
   */
  def joinReport(comicDF:DataFrame, characterDF:DataFrame,intermediateDF:DataFrame): DataFrame = {
    val union=comicDF.join(intermediateDF,comicDF.col("comicID")
      ===intermediateDF.col("comicID")).
      join(characterDF,intermediateDF.col("characterID")===characterDF.col("characterID")).
      select(comicDF.col("comicID"),comicDF.col("title"),comicDF.col("issueNumber"),
        characterDF.col("name"))

    val dataFrameFinal=union.groupBy("comicID").agg(collect_list(characterDF.col("name")))
    dataFrameFinal
  }


}
