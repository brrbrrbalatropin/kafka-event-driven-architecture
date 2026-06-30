package edu.eci.arsw.kafka.producer;

import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(PaymentProcessedEvent event) {
        kafkaTemplate.send("payments", event.getOrderId(), event);
    }
}