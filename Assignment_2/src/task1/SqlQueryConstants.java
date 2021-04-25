package task1;

public class SqlQueryConstants {

    public static final String selectQuery = "select * from olist_customers where customer_zip_code_prefix=01151 limit 20";
    public static final String updateQuery = "update olist_customers set customer_city='%s' where customer_id=";
    public static final String commitQuery = "commit";

}
