package com.pgservis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class Main {

    public static void main(String[] args) throws Exception {

        long startTime;
        long endTime;

        FillData_emp_def emp_def = new FillData_emp_def();

        SetLibPath slp = new SetLibPath();

        //Connection string for mssql x64 -- VM: "-Djava.library.path="C:\SSO\x64"
        String conMSSQLwaString = "jdbc:sqlserver://PC-COMP;databaseName=SoftComNew;integratedSecurity=true";
        //Connection string for SYBASE url login
        String conSYBASEurlString = "jdbc:sybase:Tds:172.21.21.6:2638?ServiceName=SoftCom;";

        Connection conMSSQLwa = null;
        Connection conSYBASEurl = null;

        //slp.appendToPath("C:\\SSO\\x64");
        slp.appendToPath("C:\\SSO\\x64");

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate localDate = LocalDate.now();
        String date = dtf.format(localDate);

        System.out.println("Resolver\n");
        DateFormat dtf1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date localDate1 = new Date();
        System.out.println(dtf1.format(localDate1));
        System.out.println("----------------------------------------------------");

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            try{
                conMSSQLwa = DriverManager.getConnection(conMSSQLwaString);
                System.out.println("Connection with MSSql succeed.");
            }catch (SQLException e){
                if (e.toString().contains("Connection timed out")){
                    System.out.println("Connection with MSSQL unsuccessful.");
                }else{
                    System.out.println("JDBC Error: " + e);
                }
            }

            //Connection for SYBASE url login
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            try{
                conSYBASEurl = DriverManager.getConnection(conSYBASEurlString, "installer", "installer");
                System.out.println("Connection with SyBase succeed.");
            }catch (SQLException e){
                if (e.toString().contains("Connection timed out")){
                    System.out.println("Connection with SyBase unsuccessful.");
                }else{
                    System.out.println("JDBC error: " + e);
                }
            }

            System.out.println("----------------------------------------------------");

            startTime = System.currentTimeMillis();

            emp_def.fillTable(conMSSQLwa,conSYBASEurl,date);
            System.out.println("Data inserted into table: mic_tmp_mt_emp_def_temp.");
            endTime = System.currentTimeMillis();

            System.out.println("----------------------------------------------------");
            System.out.println("Completed in " + ((endTime - startTime) / 1000) + " sec.\n");

        } catch (Exception e) {
            System.out.println("Error: " + e);
        } finally {
            if (conMSSQLwa != null){
                conMSSQLwa.close();
                System.out.println("MSSql connection closed.");
            }
            if (conSYBASEurl != null){
                conSYBASEurl.close();
                System.out.println("SyBase connection closed.");
            }
        }
    }
}