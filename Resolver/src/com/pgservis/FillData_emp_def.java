package com.pgservis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by DeXTeR on 12/14/2017.
 */
public class FillData_emp_def {

    private String sqlSy = "SELECT * FROM mic.emp_def";
    private String sqlMs = "INSERT INTO mic_tmp_mt_emp_def_temp(emp_seq,emp_bo_class_seq,cntry_seq,state_seq,bank_seq,bank_2_seq,obj_num,payroll_id,last_name,InsertDate) VALUES(?,?,?,?,?,?,?,?,?,?)";

    public void fillTable(Connection ms, Connection sy, String date) throws Exception{

        Statement statement = sy.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
        PreparedStatement pstmt = ms.prepareStatement(sqlMs);

        ms.setAutoCommit(false);

        ResultSet rs = statement.executeQuery(sqlSy);

        final int batchSize = 1000;
        int count = 0;

        while (rs.next()) {
            pstmt.setString(1, rs.getString(1));
            pstmt.setString(2, rs.getString(2));
            pstmt.setString(3, rs.getString(3));
            pstmt.setString(4, rs.getString(4));
            pstmt.setString(5, rs.getString(5));
            pstmt.setString(6, rs.getString(6));
            pstmt.setString(7, rs.getString(7));
            pstmt.setString(8, rs.getString(8));
            pstmt.setString(9, rs.getString(9));
            pstmt.setString(10, date);
            pstmt.addBatch();

            if(++count % batchSize == 0) {
                pstmt.executeBatch();
            }
        }

        pstmt.executeBatch();
        ms.commit();
        pstmt.close();
    }
}
