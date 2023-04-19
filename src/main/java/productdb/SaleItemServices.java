package productdb;

import com.example.DistributedAssignment.cutomer_db.services.*;
import com.example.DistributedAssignment.cutomer_db.services.Void;
import io.grpc.stub.StreamObserver;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SaleItemServices extends SaleItemServicesGrpc.SaleItemServicesImplBase {

    @Override
    public void putItemForSale(SaleItem request, StreamObserver<Void> responseObserver) {
        try {
            Connection connection = Utils.getConnectionToProductDB();
            String query = "INSERT INTO Products(\"itemName\",\"category\",\"keyWords\",\"isNew\",\"itemPrice\",\"sellerID\",\"quantity\") VALUES (\""
                    + request.getItemName() + "\", " + request.getCategory() + ", \"" + request.getKeyWords() + "\", " + request.getIsNew() + ", "
                    + request.getItemPrice() + ", " + request.getSellerID() + ", " + request.getQuantity() + ")";
            connection.createStatement().execute(query);
            connection.close();
            responseObserver.onNext(Void.newBuilder().build());
            responseObserver.onCompleted();
        } catch (SQLException exception) {
            responseObserver.onError(exception);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void changeSalePriceOfItem(ChangeSalePriceRequest request, StreamObserver<Void> responseObserver) {
        try {
            Connection connection = Utils.getConnectionToProductDB();
            String query = "UPDATE Products SET \"itemPrice\" =" + request.getNewPrice() + " WHERE \"itemID\" = "
                    + request.getItemID() + " AND \"sellerID\" = " + request.getSellerID();
            connection.createStatement().execute(query);
            connection.close();
            responseObserver.onNext(Void.newBuilder().build());
            responseObserver.onCompleted();

        } catch (SQLException exception) {
            responseObserver.onError(exception);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void removeItemFromSale(RemoveItemFromSaleRequest request, StreamObserver<Void> responseObserver) {
        try {
            Connection connection = Utils.getConnectionToProductDB();
            String query = "SELECT quantity FROM Products WHERE \"itemID\" = " + request.getItemID() + " AND \"sellerID\" = " + request.getSellerID();
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            int currentSaleQuantityForItem = 0;
            int quantity = request.getQuantity();
            if (resultSet.next()) {
                currentSaleQuantityForItem = resultSet.getInt(1);
                int sellerID = request.getSellerID();
                int itemID = request.getItemID();
                if (currentSaleQuantityForItem < quantity) {
                    responseObserver.onNext(Void.newBuilder().build());
                    responseObserver.onCompleted();
                    connection.close();
                    return;
                } else if (currentSaleQuantityForItem == quantity) {
                    query = "DELETE FROM Products WHERE \"sellerID\" = " + sellerID + " and \"itemID\" = \"" + itemID + "\"";
                } else {
                    query = "UPDATE Products SET \"quantity\" = " + (currentSaleQuantityForItem - quantity) + " WHERE \"sellerID\" = " + sellerID + " and \"itemID\" = \"" + itemID + "\"";
                }
                connection.createStatement().execute(query);
                connection.close();
            }
            responseObserver.onNext(Void.newBuilder().build());
            responseObserver.onCompleted();

        } catch (SQLException exception) {
            responseObserver.onError(exception);
            responseObserver.onCompleted();
        }

    }

    @Override
    public void displayItemsOnSale(UserID request, StreamObserver<SaleItem> responseObserver) {
        try {
            Connection connection = Utils.getConnectionToProductDB();
            String query = "SELECT itemName,category,keyWords,isNew,itemPrice,sellerID,quantity,itemID FROM Products WHERE \"sellerID\" = " + request.getUserId();
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            while (resultSet.next()) {
                SaleItem saleItem = SaleItem.newBuilder()
                        .setItemName(resultSet.getString(1))
                        .setCategory(resultSet.getInt(2))
                        .setKeyWords(resultSet.getString(3))
                        .setIsNew(resultSet.getInt(4))
                        .setItemPrice(resultSet.getFloat(5))
                        .setSellerID(resultSet.getInt(6))
                        .setQuantity(resultSet.getInt(7))
                        .setItemID(resultSet.getInt(8))
                        .build();
                responseObserver.onNext(saleItem);
            }
            connection.close();
        } catch (SQLException exception) {
            responseObserver.onError(exception);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getItemDetails(ItemID request, StreamObserver<SaleItem> responseObserver) {
        try {
            Connection connection = Utils.getConnectionToProductDB();
            String query = "SELECT \"itemName\",\"category\",\"keyWords\",\"isNew\",\"itemPrice\",\"sellerID\",\"quantity\""
                    + " FROM Products WHERE \"itemID\" = " + request.getItemId();
            ResultSet productResultSet = connection.createStatement().executeQuery(query);
            SaleItem saleItem = SaleItem.newBuilder()
                    .setItemName(productResultSet.getString(1))
                    .setCategory(productResultSet.getInt(2))
                    .setKeyWords(productResultSet.getString(3))
                    .setIsNew(productResultSet.getInt(4))
                    .setItemPrice(productResultSet.getFloat(5))
                    .setSellerID(productResultSet.getInt(6))
                    .setQuantity(productResultSet.getInt(7))
                    .build();
            responseObserver.onNext(saleItem);
        }catch (SQLException exception) {
            responseObserver.onError(exception);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void searchItemsForSale(SearchRequest request, StreamObserver<SaleItem> responseObserver) {
        try {
            Connection connection = Utils.getConnectionToProductDB();
            String keywords = request.getKeywords();
            String[] keywordsList = keywords.split(":");
            StringBuilder builder = new StringBuilder();

            builder.append("SELECT \"itemName\",\"category\",\"keyWords\",\"isNew\",\"itemPrice\",\"sellerID\"," +
                    "\"quantity\" , \"itemID\" FROM Products where \"category\" = ").append(request.getCategory()).append(" AND ");
            for (String keyword : keywordsList) {
                builder.append("\"keywords\" LIKE \"%").append(keyword).append("%\"");
                builder.append(" OR ");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.deleteCharAt(builder.length() - 1);
            builder.deleteCharAt(builder.length() - 1);
            String query = builder.toString();
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            while (resultSet.next()) {
                SaleItem saleItem = SaleItem.newBuilder()
                        .setItemName(resultSet.getString(1))
                        .setCategory(resultSet.getInt(2))
                        .setKeyWords(resultSet.getString(3))
                        .setIsNew(resultSet.getInt(4))
                        .setItemPrice(resultSet.getFloat(5))
                        .setSellerID(resultSet.getInt(6))
                        .setQuantity(resultSet.getInt(7))
                        .setItemID(resultSet.getInt(8))
                        .build();
                responseObserver.onNext(saleItem);
            }
            connection.close();
        } catch (SQLException exception) {
            responseObserver.onError(exception);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getSellerIDForItem(ItemID request, StreamObserver<UserID> responseObserver) {
        try {
            Connection connection = Utils.getConnectionToProductDB();
            int sellerID = -1;
            String query = "SELECT sellerID FROM Products WHERE \"itemID\" = "+ request.getItemId();
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.next()){
                sellerID = resultSet.getInt(1);
            }
            responseObserver.onNext(UserID.newBuilder().setUserId(sellerID).build());
            connection.close();
        } catch (SQLException exception){
            responseObserver.onError(exception);
        }
        responseObserver.onCompleted();

    }
}
