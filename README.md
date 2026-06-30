# Laboratory Activity: Event-Driven Architecture with Apache Kafka

Colombian School of Engineering Julio Garavito

Software Architectures

Diego Alejandro Montes

David Felipe Rayo

## Introduction and Context
This project explores the implementation of an event-driven architecture focused on high performance, microservices decoupling, and reliable message processing. In modern software systems, it is crucial to handle large volumes of events without losing critical data, keeping services independent. We use Apache Kafka as the main event streaming platform to achieve this decoupling between producers and consumers. The central scenario revolves around an e-commerce system that processes orders, payments, and inventory reservations asynchronously. Ensuring that each order is correctly processed in parallel by multiple domains is the main challenge of this architecture.

## Detailed Requirements
The instructions for this laboratory activity required the creation of a distributed messaging architecture using the choreography pattern. It required implementing an order producer service and multiple logical consumer services for payments and inventory interacting through dedicated topics. The independence of these components ensures that the system can continue accepting orders even if the inventory or payments system is temporarily offline.

We developed these applications using Java and Spring Boot. The producer is responsible for receiving web requests and immediately publishing an order created event. The consumer applications must read these events in real time, execute their business logic, such as approving a payment based on the amount, and publish the result to their respective payments and inventory topics. Finally, the activity required applying error recovery strategies, such as the use of dead letter topics, to ensure that no message blocks the system and that financial and inventory data are not lost.

## Implementation Strategy
The implementation was executed in three distinct and sequential phases to ensure an organized workflow.

The first phase consisted of configuring the infrastructure using a containerized environment with Docker Compose. We deployed a Kafka cluster in a standalone (Kraft/no external coordinator) mode along with a graphical interface to inspect events in real time.

The second phase focused on writing the logic for the Java applications using the Spring framework for Kafka. We built the complete flow by defining data transfer object classes for the events and using Kafka templates for message publishing. Simultaneously, we built the consumers using Kafka listener annotations, assigning each a distinct group identifier (payments service and inventory service), so that both receive their own copy of the order created event under the publish-subscribe pattern.

The third phase consisted of testing the resilience and behavior of the system. Through web requests, we sent simulated orders with different values and monitored in the user interface how a single web event branched into multiple asynchronous events processed by different consumer groups, thus validating the eventual consistency of the data.

## Design Decisions
Apache Kafka was selected due to its ability to handle immutable log-based messaging, allowing for message retention and reprocessing in case of failures. We used Spring for Kafka because it offers a straightforward, declarative approach for operations, avoiding the complexity of manually configuring low-level clients.

A key architectural decision was to reject the use of a single global events topic to avoid the firehose anti-pattern, where consumers wake up unnecessarily to discard messages that do not belong to them. Instead, we isolated the events in dedicated topics for orders, payments, and inventory. Additionally, we established the order identifier as the partitioning key for all events, ensuring that all occurrences of the same order are processed in strict sequential order within the same partition.

## Evidence and Execution

### 1. Stand Up the Infrastructure
To start Kafka and its graphical interface, open your console and run the command to start Docker Compose in the project root, making sure you have Docker running.
Command: `docker compose up -d`
Once started, the interface will be available on local port 8080.

### 2. Run the Spring Boot Application
With the infrastructure ready, run the Java application making sure you have Java 21 and Maven installed.
Command: `mvn spring-boot:run`

### 3. Generate Producer Events in Action
You can send a new order to the system via a creation request through the command terminal to observe the system in real time by sending a customer identifier and a numerical total value to port 8081 on the orders path.
Command: `curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d '{"customerId":"CUS01","total":120000}'`

### 4. Consumption and Choreography Verification
Once the command is sent, in the application console you will see the reception and processing of the events with approved and reserved status.

In the Kafka graphical interface, you can visually inspect the following:
Topics: Verify that messages exist in the orders, payments, and inventory topics.
Consumers: You will be able to observe the payments service and inventory service groups successfully consuming events, demonstrating successful and completely decoupled real-time processing.
