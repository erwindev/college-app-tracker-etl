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
        Dataset<Row> hd_df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("files/hd2016.csv")

        Dataset<Row> fdf = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("files/hd2016-Frequencies.csv")

        Dataset<Row> staff_df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("files/s2015_oc.csv")

        Dataset<Row> tution_df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("files/ic2016_ay.csv")

        sqlContext.registerDataFrameAsTable(hd_df, "college")
        sqlContext.registerDataFrameAsTable(staff_df, "staff")
        sqlContext.registerDataFrameAsTable(tution_df, "tuition")
        sqlContext.registerDataFrameAsTable(fdf, "college_lookup_values")

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

        //Define processStaffCat method
        sqlContext.udf().register("processStaffCat", new UDF1<Integer, String>() {
            @Override
            public String call(Integer cat) throws Exception {
                String val = "NONE"
                switch(cat){
                    case 2200:
                        val = "TOTAL_FULLTIME"
                        break
                    case 2210:
                        val = "TOTAL_FULLTIME_INSTRUCTIONAL"
                        break
                    case 2220:
                        val = "TOTAL_FULLTIME_RESEARCH"
                        break
                    case 2230:
                        val = "TOTAL_FULLTIME_PUBLIC"
                        break
                    case 3200:
                        val = "TOTAL_PARTTIME"
                        break
                    case 3210:
                        val = "TOTAL_PARTTIME_INSTRUCTIONAL"
                        break
                    case 3220:
                        val = "TOTAL_PARTTIME_RESEARCH"
                        break
                    case 3230:
                        val = "TOTAL_PARTTIME_PUBLIC"
                        break
                }
                return val
            }
        }, DataTypes.StringType)

        //Lookup value of State Abbr and transform to Full State Name
        Dataset<Row> collegeDs = sqlContext.sql("select a.UNITID, a.INSTNM, a.ADDR, a.CITY, " +
                "b.valuelabel STATE, " +
                "a.ZIP, " +
                "c.valuelabel REGION, " +
                "d.valuelabel SECTOR,  " +
                "a.GENTELE TELEPHONE, " +
                "a.WEBADDR WEB_URL, " +
                "a.ADMINURL ADMISSION_URL, " +
                "a.APPLURL APPLICATION_URL, " +
                "e.valuelabel LOCALE, " +
                "a.LONGITUD, " +
                "a.LATITUDE " +
                "from college a, college_lookup_values b, college_lookup_values c, college_lookup_values d, college_lookup_values e " +
                "where b.codevalue = a.STABBR " +
                "and c.codevalue = a.OBEREG " +
                "and d.codevalue = a.SECTOR " +
                "and e.codevalue = a.LOCALE " +
                "and b.varname = 'STABBR' " +
                "and c.varname = 'OBEREG' " +
                "and d.varname = 'SECTOR' " +
                "and e.varname = 'LOCALE' ").toDF()
        collegeDs.show(3)

        Dataset<Row> staffDs = sqlContext.sql(
                "select " +
                        "a.UNITID, " +
                        "a.HRTOTLT, " +
                        "processStaffCat(a.STAFFCAT) TYPE " +
                        "from staff a " +
                        "where " +
                        "a.STAFFCAT in (2200, 2210, 2220, 2230, 3200, 3210, 3220, 3230) ").toDF()

        staffDs.show(20)

        Dataset<Row> tutionDs = sqlContext.sql(
                "select " +
                        "a.UNITID, " +
                        "a.TUITION2 IN_STATE_TUITION_UNDERGRAD, " +
                        "a.FEE2 IN_STATE_FEE_UNDERGRAD, " +
                        "a.TUITION3 OUT_OF_STATE_TUITION_UNDERGRAD, " +
                        "a.FEE3 OUT_OF_STATE_FEE_UNDERGRAD, " +
                        "a.TUITION6 IN_STATE_TUITION_GRAD, " +
                        "a.FEE6 IN_STATE_FEE_GRAD, " +
                        "a.TUITION7 OUT_OF_STATE_TUITION_GRAD, " +
                        "a.FEE7 OUT_OF_STATE_FEE_GRAD " +
                        "from tuition a ").toDF()

        tutionDs.show(20)

        collegeDs = collegeDs.join(tutionDs, "UNITID")

        /*
        LOAD (write to postgress database)
         */
        Properties props = new Properties()
        props.setProperty('user', dbUser)
        props.setProperty('password', dbPassword)
        props.setProperty("driver", "org.postgresql.Driver")

        collegeDs.write().mode('overwrite').jdbc(dbUrl, 'college', props)
        staffDs.write().mode('overwrite').jdbc(dbUrl, 'staff', props)

        sc.stop()


    }

}
