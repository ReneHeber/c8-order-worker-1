package com.example.c8orderworker;

import com.example.c8orderworker.handler.OrderHandler;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.worker.JobWorker;
import io.camunda.zeebe.client.impl.oauth.OAuthCredentialsProvider;
import io.camunda.zeebe.client.impl.oauth.OAuthCredentialsProviderBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

@SpringBootApplication
public class C8OrderWorkerApplication {

    // Zeebe Client Credentials
    private static final String ZEEBE_PROPERTIES_PATH = "src/main/resources/application.properties";
    private static String ZEEBE_ADDRESS;
    private static String ZEEBE_CLIENT_ID;
    private static String ZEEBE_CLIENT_SECRET;
    private static String ZEEBE_TOKEN_AUDIENCE = "zeebe.camunda.io";
    private static String ZEEBE_AUTHORIZATION_SERVER_URL;
    private static String ZEEBE_REST_ADDRESS;
    private static String ZEEBE_GRPC_ADDRESS;

    // Job
    private static final String JOB_TYPE = "trackOrderStatus";

    public static void main(String[] args) {
        // SpringApplication.run(C8OrderWorkerApplication.class, args);
        loadProperties();

        final OAuthCredentialsProvider credentialsProvider = new OAuthCredentialsProviderBuilder()
                .audience(ZEEBE_TOKEN_AUDIENCE)
                .clientId(ZEEBE_CLIENT_ID)
                .clientSecret(ZEEBE_CLIENT_SECRET)
                .authorizationServerUrl(ZEEBE_AUTHORIZATION_SERVER_URL)
                .build();

        try {
            System.setProperty("javax.net.ssl.trustStore", "path/to/truststore.jks");
            System.setProperty("javax.net.ssl.trustStorePassword", "truststorePassword");

            final ZeebeClient client = ZeebeClient.newClientBuilder()
                    .gatewayAddress(ZEEBE_ADDRESS)
                    .credentialsProvider(credentialsProvider)
                    .usePlaintext() // Use plaintext for communication (recommended for testing purposes only)
                    .build();

            client.newTopologyRequest().send().join();
            System.out.println("Connected to: " + client.newTopologyRequest().send().join());

            // Start a Job Worker
            System.out.println("Opening job worker.");
            final JobWorker downloadPictureWorker = client.newWorker()
                    .jobType(JOB_TYPE)
                    .handler(new OrderHandler())
                    .timeout(Duration.ofSeconds(3000))
                    .open();

            System.out.println("Job worker opened and receiving jobs.");

            // Waiting to maintain the connection
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Close client when the program is terminated
            client.close();

            // run until System.in receives exit command
            // waitUntilSystemInput("exit");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void loadProperties() {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(ZEEBE_PROPERTIES_PATH)) {
            properties.load(input);
            ZEEBE_ADDRESS = properties.getProperty("zeebe.client.cloud.address");
            ZEEBE_CLIENT_ID = properties.getProperty("zeebe.client.cloud.clientId");
            ZEEBE_CLIENT_SECRET = properties.getProperty("zeebe.client.cloud.clientSecret");
            ZEEBE_AUTHORIZATION_SERVER_URL = properties.getProperty("zeebe.authorization.server.url");
            ZEEBE_REST_ADDRESS = properties.getProperty("zeebe.rest.address");
            ZEEBE_GRPC_ADDRESS = properties.getProperty("zeebe.grpc.address");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void waitUntilSystemInput(String exitCommand) {
        try {
            System.out.println("Type '" + exitCommand + "' to exit the application.");
            while (true) {
                int readByte = System.in.read();
                if (readByte != -1 && Character.toLowerCase((char) readByte) == exitCommand.charAt(0)) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

