package com.guavus.wildcard;


import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.UUID;

/**
 * Created by mina on 8/14/15.
 */
public class Main {

    public static String INSERT_SQL = "INSERT INTO carereflex_suddenlink.wildcard_account(name, account) VALUES ";

    public static void main(String[] args){

        try {

            PrintWriter writer = new PrintWriter("/home/mina/wildcard.csv", "UTF-8");

            for(int i=0;i<1000000;++i)
                writer.println("WILDCARD," + generateRandomAccountId());

            writer.close();



        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

    }

    public static void main2(String[] args){
        SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
        dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        dataSource.setUrl("jdbc:hive2://devqa-lb-01:21050/;auth=noSasl");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        createWildcards(jdbcTemplate, 10000);

        jdbcTemplate.execute("");

    }

    private static void createWildcards(JdbcTemplate jdbcTemplate, int count) {
        String wildcard = "wildcardxxxs_";

        for(int i=0;i<count;i++){
            System.out.print("Create Wildcard \"" + wildcard + (i + 1) + "\" ....");
            createWildcard(jdbcTemplate, wildcard + (i + 1), 1000000);
            System.out.println("      [ OK ]");
        }

    }

    private static void createWildcard(JdbcTemplate jdbcTemplate, String wildcardName, int accountCount) {

        int batchSize = 500;
        String sql = INSERT_SQL;

        for(int i=1;i<=accountCount;i++){

            sql+= "('" + wildcardName + "','" + generateRandomAccountId() + "') ";

            if(i%batchSize == 0){
                jdbcTemplate.execute(sql);
                sql = INSERT_SQL;
            }
            else if( i != accountCount-1 ){
                sql+= " , ";
            }
        }

    }

    private static String generateRandomAccountId() {
        return UUID.randomUUID().toString().substring(0,12);
    }
}
