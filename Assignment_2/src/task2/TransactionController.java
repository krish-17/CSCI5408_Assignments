package task2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionController {

    public static void main (String[] args) {

        String[] transaction1 = new String[] {
                "update olist_orders set order_status='delivered' where order_id='00bca4adac549020c1273714d04d0208'",
                "update olist_customers set customer_city='sao paulo', customer_zip_code_prefix=4841 where customer_id='02d34c287580a6f46ad7ffc395bbc208'",
                "insert into olist_sellers values('3442f8959a84dea7ee197c632cb2df45', 13023, 'campinas', 'SP')",
                "update olist_sellers set seller_city='sao paulo', seller_zip_code_prefix=4841 where seller_id='3442f8959a84dea7ee197c632cb2df45'",
                "delete from olist_sellers where seller_id='3442f8959a84dea7ee197c632cb2df45'"
        };
        String[] tableName1 = new String[] {"olist_customers", "olist_customers", "olist_sellers", "olist_sellers", "olist_sellers" };
        String[] transaction2 = new String[] {
                "update olist_orders set order_delivered_carrier_date='2017-02-14 18:03:38' where order_id='00bca4adac549020c1273714d04d0208'",
                "update olist_customers set customer_city='rio de janeiro', customer_state='RJ', customer_zip_code_prefix=20540 where customer_id='0002414f95344307404f0ace7a26f1d5'",
                "update olist_order_reviews set review_comment_title='Amazing product' where review_id='7bc2406110b926393aa56f80a40eba40'",
                "update olist_order_reviews set review_comment_message='Delivered rightly on time. love Data5408' where review_id='7bc2406110b926393aa56f80a40eba40'",
                "delete from olist_products where product_id='9dc1a7de274444849c219cff195d0b71'"
        };
        String[] tableName2 = new String[] {"olist_orders", "olist_customers", "olist_order_reviews", "olist_products", "olist_products"};
        GlobalSchemaParser glbParser = new GlobalSchemaParser();
        glbParser.populateTableMap();
        implementTransactions(transaction1, tableName1, glbParser);
        implementTransactions(transaction2, tableName2, glbParser);
    }

    private static void implementTransactions( String[] transactions, String[] tables, GlobalSchemaParser glbParser) {
        DBConnector trCon = null;
        WALImplementer wal = new WALImplementer();
        HashMap<String, DBConnector> schemaConnectorObj = new HashMap<>();
        List<Long> sequenceIds = new ArrayList<>();
        for( int i = 0; i < transactions.length; i++) {
            long sequenceId = System.currentTimeMillis();
            sequenceIds.add(sequenceId);
            String[] dbProps = glbParser.getDbPropsByTableName(tables[i]);
            if (! schemaConnectorObj.containsKey(dbProps[0])) {
                trCon = new DBConnector(dbProps[0], dbProps[1], dbProps[2]);
                trCon.connectDB(true);
                schemaConnectorObj.put(dbProps[0], trCon);
            }
            if (trCon.executeUID(transactions[i])) {
                wal.setWALProps(transactions[i], transactions[i], true, sequenceId);
                wal.writeWAL();
            } else {
                performRollback(schemaConnectorObj, wal, sequenceIds.get(0));
                break;
            }
            System.out.println("Executed:" + transactions[i]);
        }
        for(Map.Entry schemaUrl : schemaConnectorObj.entrySet()) {
            DBConnector conObj = (DBConnector)schemaUrl.getValue();
            conObj.executeCommit();
            System.out.println("Transaction commited for:" + schemaUrl.getKey());
            conObj.closeConnection();
        }

    }

    private static void performRollback(HashMap<String, DBConnector> schemaConnectorObj, WALImplementer wal, long seqId ) {
        for(Map.Entry schemaUrl : schemaConnectorObj.entrySet()) {
            DBConnector conObj = (DBConnector)schemaUrl.getValue();
            conObj.executeRollBack();
            conObj.closeConnection();
            wal.notifyRollBack(seqId);
        }

    }
}
