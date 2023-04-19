package productdb;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class ProductDBServer {
    public static void main(String[] args) throws InterruptedException, IOException {
        Server server = ServerBuilder.forPort(Utils.PRODUCT_DB_PORT).addService(new SaleItemServices()).build();
        server.start();
        server.awaitTermination();
    }
}
