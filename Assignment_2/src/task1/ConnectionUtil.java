package task1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ConnectionUtil {

    private Connection conn;

    public ConnectionUtil() {
        try {
            this.conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/Data_5408_local", "root", "@Jaya1961");
            conn.setAutoCommit(false); //To perform transactions
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void closeConnection() {
        try {
            this.conn.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void commitConnection() {
        try {
            this.conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public ResultSet executeRead(String query) {
        try {
            Statement stm = this.conn.createStatement();
            return stm.executeQuery(query);
        } catch (Exception e) {
            return null;
        }

    }

    public void executeBatchUpdate(ResultSet rs, String q) {
        try {
            Statement stmt = this.conn.createStatement();
            while (rs != null && rs.next()) {
                String query = q + "'" + rs.getString("customer_id") + "'";
                stmt.addBatch(query);
            }
            stmt.executeBatch();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }



}
