package task1;

import java.sql.*;

public class TransactionImpl {

    ResultSet getResultSet(ConnectionUtil cu, ResultSet rs, int i, String[] seqQuery) throws SQLException {
        if (seqQuery[i].contains("select")) {
            rs = cu.executeRead(seqQuery[i]);
        }
        if (seqQuery[i].contains("update")) {
            cu.executeBatchUpdate(rs, seqQuery[i]);
        }
        if (seqQuery[i].equalsIgnoreCase("")) {
        }
        if (seqQuery[i].equalsIgnoreCase(SqlQueryConstants.commitQuery)) {
            cu.commitConnection();
        }
        return rs;
    }

    synchronized void executeQueryStatement(String[] seqQuery, String number, ConnectionUtil[] cu) {
        try {
            ResultSet rs = null;
            for(int i = 0; seqQuery.length > i; i++) {
                System.out.println("Transaction:" + i + " Sequence:" + number);
                rs = getResultSet(cu[i], rs, i, seqQuery);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
