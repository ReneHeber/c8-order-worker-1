package com.example.c8orderworker;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;
import io.camunda.zeebe.client.api.worker.JobWorker;
import io.camunda.zeebe.client.impl.oauth.OAuthCredentialsProvider;
import io.camunda.zeebe.client.impl.oauth.OAuthCredentialsProviderBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class SimpleJobWorker {

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
        // Verbindung zum Zeebe-Broker herstellen
        loadProperties();

        final OAuthCredentialsProvider credentialsProvider = new OAuthCredentialsProviderBuilder()
                .audience(ZEEBE_TOKEN_AUDIENCE)
                .clientId(ZEEBE_CLIENT_ID)
                .clientSecret(ZEEBE_CLIENT_SECRET)
                .authorizationServerUrl(ZEEBE_AUTHORIZATION_SERVER_URL)
                .build();

        try {
            // Verbindung zum Zeebe-Broker herstellen
            ZeebeClient client = ZeebeClient.newClientBuilder()
                    .gatewayAddress(ZEEBE_ADDRESS)
                    .credentialsProvider(credentialsProvider)
                    .usePlaintext() // Use plaintext for communication (recommended for testing purposes only)
                    .build();

            client.newTopologyRequest().send().join();
            System.out.println("Connected to: " + client.newTopologyRequest().send().join());

            // Start a Job Worker
            System.out.println("Opening job worker.");
            JobWorker worker = client.newWorker()
                    .jobType(JOB_TYPE)
                    .handler(new SampleJobHandler())
                    .timeout(Duration.ofSeconds(10000))
                    .open();

            System.out.println("Job worker opened and receiving jobs.");

            // Worker laufen lassen, bis das Programm beendet wird
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Worker wird heruntergefahren...");
                worker.close();
                client.close();
                System.out.println("Worker heruntergefahren.");
            }));

        } catch (Exception e) {
            System.out.println("Fehler beim Herstellen der Verbindung zum Broker: " + e.getMessage());
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

    static class SampleJobHandler implements JobHandler {
        @Override
        public void handle(JobClient client, io.camunda.zeebe.client.api.response.ActivatedJob job) {

            try {
                // Hier wird die eigentliche Verarbeitung der Aufgabe durchgeführt
                System.out.println("Bearbeite Aufgabe mit ID: " + job.getKey());

                // Beispiel: Markiere die Aufgabe als abgeschlossen
                client.newCompleteCommand(job.getKey()).send().join();

            } catch(Exception e) {
                System.err.println("Error handling job: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}