package com.run.dao;

import com.run.pojo.CallDate;
import com.run.pojo.CallPeople;
import com.run.utils.JDBCInstance;
import scala.util.Try;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DimesionImpl implements Dimesion{


    /**
     * 返回联系人表的查询和插入语句
     *
     * @return
     */
    private String[] getContactDimensionSQL() {
        String query = "SELECT `id` FROM `tb_contacts` WHERE `telephone` = ? AND `name` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_contacts` (`telephone`, `name`) VALUES (?, ?);";
        return new String[]{query, insert};
    }

    /**
     * 返回时间表的查询和插入语句
     *
     * @return
     */
    private String[] getDateDimensionSQL() {
        String query = "SELECT `id` FROM `tb_dimension_date` WHERE `year` = ? AND `month` = ? AND `day` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_date` (`year`, `month`, `day`) VALUES (?, ?, ?);";
        return new String[]{query, insert};
    }

    /**
     * 得到当前线程维护的Connection对象
     * @return
     */
    private Connection getConnection() {
        Connection conn = null;
        try {
            if (conn == null || conn.isClosed()) {
                conn = JDBCInstance.getInstance();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 手机号 姓名
     * @param people
     * @return
     */
    @Override
    public int getDimensionID(CallPeople people){
        int id = -1;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            String[] sql = getContactDimensionSQL();
            String ins = sql[1];
            String sel = sql[0];
            Connection connection = getConnection();
            // 先查询
            preparedStatement = connection.prepareStatement(sel);
            preparedStatement.setString(2,people.getName());
            preparedStatement.setString(1,people.getPhoneNum());
            resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                id = resultSet.getInt(1);
            }
            if(id == -1){
                preparedStatement = connection.prepareStatement(ins);
                preparedStatement.setString(2,people.getName());
                preparedStatement.setString(1,people.getPhoneNum());
                preparedStatement.executeUpdate();
                // 在查
                preparedStatement = connection.prepareStatement(sel);
                preparedStatement.setString(2,people.getName());
                preparedStatement.setString(1,people.getPhoneNum());
                resultSet = preparedStatement.executeQuery();
                if(resultSet.next()){
                    id = resultSet.getInt(1);
                }
            }


            // 查询

        } catch (Exception e) {
            e.printStackTrace();
        }
        // 插入
        return id;
    }

    @Override
    public int getDimensionID(CallDate date) {
        int id = -1;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            String[] sql = getDateDimensionSQL();
            String ins = sql[1];
            String sel = sql[0];
            Connection connection = getConnection();
            // 先查询
            // 查询
            preparedStatement = connection.prepareStatement(sel);
            preparedStatement.setString(1,date.getYear());
            preparedStatement.setString(2,date.getMonth());
            preparedStatement.setString(3,date.getDay());
            resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                id = resultSet.getInt(1);
            }
            if(id == -1){
                preparedStatement = connection.prepareStatement(ins);
                preparedStatement.setString(1,date.getYear());
                preparedStatement.setString(2,date.getMonth());
                preparedStatement.setString(3,date.getDay());
                preparedStatement.executeUpdate();
                // 在查
                preparedStatement = connection.prepareStatement(sel);
                preparedStatement.setString(1,date.getYear());
                preparedStatement.setString(2,date.getMonth());
                preparedStatement.setString(3,date.getDay());
                resultSet = preparedStatement.executeQuery();
                if(resultSet.next()){
                    id = resultSet.getInt(1);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        // 插入
        return id;
    }


}
