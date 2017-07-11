package com.erwindev.college

import com.erwindev.college.util.ApplicationProperties
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes

/**
 * Created by erwinalberto on 7/10/17.
 */
class CollegeETL {

    static void main(String[] args){

        String dbUrl = ApplicationProperties.getValue('college.etl.db.url')
        String dbUser = ApplicationProperties.getValue('college.etl.db.user')
        String dbPassword = ApplicationProperties.getValue('college.etl.db.password')

        SparkConf conf = new SparkConf().setMaster("local").setAppName("College App")
        JavaSparkContext sc = new JavaSparkContext(conf)

        SQLContext sqlContext = new SQLContext(sc)

        /*
        EXTRACT
         */
        Dataset<Row> df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("files/hd2015.csv")

        Dataset<Row> fdf = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("files/hd2015-Frequencies.csv")

        sqlContext.registerDataFrameAsTable(df, "college")
        sqlContext.registerDataFrameAsTable(fdf, "lookup_values")

        /*
        TRANSFORM
         */

        //Define randomUUID method
        sqlContext.udf().register("randomUUID", new UDF1<String, String>() {
            @Override
            public String call(String str) throws Exception {
                return UUID.randomUUID().toString()
            }
        }, DataTypes.StringType)

        //Define addColumn method
        sqlContext.udf().register("addColumn", new UDF1<String, String>() {
            @Override
            public String call(String str) throws Exception {
                return str
            }
        }, DataTypes.StringType)

        //Define processUnitId method
        sqlContext.udf().register("processUnitId", new UDF1<Integer, Integer>() {
            @Override
            public Integer call(Integer intg) throws Exception {
                if (intg.intValue() == 100636){
                    return Integer.valueOf(123456)
                }
                return intg
            }
        }, DataTypes.IntegerType)

        //Add SZ_CUSTID column
        //Add a UUID column
        //Lookup value of State Abbr and transform to Full State Name
        Dataset<Row> df1 = sqlContext.sql("select randomUUID('') ID, " +
                "b.valuelabel STATE, " +
                "addColumn('3003') SZ_CUSTID, a.* " +
                "from college a, lookup_values b " +
                "where b.codevalue = a.STABBR " +
                "and b.varname = 'STABBR'").toDF()
        df1.show()

        /*
        LOAD (write to postgress database)
         */
        Properties props = new Properties()
        props.setProperty('user', dbUser)
        props.setProperty('password', dbPassword)
        props.setProperty("driver", "org.postgresql.Driver")

        //Write to the database or Write back to S3 as an AVRO file
        df1.write().mode('overwrite').jdbc(dbUrl, 'college_etl', props)

        //send message a queue i'm done

        sc.stop()



    }

}
