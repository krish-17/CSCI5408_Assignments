package task2;

import java.sql.*;

public class DBConnector {


    private String schemaUrl;
    private String userName;
    private String pass;
    private Connection connector;
    private Statement stmt;

    public DBConnector(String schemaUrl, String userName, String pass) {
        this.schemaUrl = schemaUrl;
        this.userName = userName;
        this.pass = pass;
        this.connector = null;
    }

    public void connectDB(boolean isTransactions) {
        try {
            this.connector = DriverManager.getConnection(this.schemaUrl, this.userName, this.pass);
            this.connector.setAutoCommit(!isTransactions);
        } catch (Exception e) {
            System.out.println("Error establishing connection");
            e.printStackTrace();
        }
    }

    public ResultSet executeSelect(String query) {
        try {
            Statement stmt = this.connector.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            return rs;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean executeUID(String query) {
        try {
            Statement stmt = this.connector.createStatement();
            stmt.executeUpdate(query);
            return true;
        } catch(Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean executeCommit() {
        try {
            this.connector.commit();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean closeConnection() {
        try {
            this.connector.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean executeRollBack() {
        try{
            this.connector.rollback();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
