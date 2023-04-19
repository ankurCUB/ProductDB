package productdb;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public interface Utils {

    int PRODUCT_DB_PORT = 8085;

    static Connection getConnectionToProductDB() throws SQLException {
        File file = new File("ProductDB.db");
        String url = "jdbc:sqlite:"+file.getAbsolutePath();
        if(file.exists()) {
            return DriverManager.getConnection(url);
        } else {
            Connection connection = DriverManager.getConnection(url);
            connection.createStatement().execute("CREATE TABLE \"Products\" (\n" +
                    "\t\"itemID\"\tINTEGER NOT NULL,\n" +
                    "\t\"itemName\"\tTEXT NOT NULL,\n" +
                    "\t\"category\"\tINTEGER NOT NULL,\n" +
                    "\t\"keywords\"\tTEXT,\n" +
                    "\t\"isNew\"\tINTEGER NOT NULL,\n" +
                    "\t\"itemPrice\"\tREAL NOT NULL,\n" +
                    "\t\"sellerID\"\tINTEGER NOT NULL,\n" +
                    "\t\"quantity\"\tINTEGER NOT NULL,\n" +
                    "\tPRIMARY KEY(\"itemID\" AUTOINCREMENT)\n" +
                    ")");
            return connection;
        }
    }
}
