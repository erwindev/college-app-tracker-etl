package com.erwindev.college.util

/**
 * Created by erwinalberto on 7/10/17.
 */
class ApplicationProperties {

    private static final ApplicationProperties instance = new ApplicationProperties()
    Properties prop = new Properties()

    private ApplicationProperties(){
        loadPropertyFile()
    }

    static ApplicationProperties getInstance(){
        return instance
    }

    private void loadPropertyFile(){
        String filename = 'application.properties'
        InputStream input = this.class.classLoader.getResourceAsStream(filename)
        if (input == null){
            return
        }

        prop.load(input)
    }

    public static String getValue(String key){
        return instance.prop.getProperty(key)
    }


}
