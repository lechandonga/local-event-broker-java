package com.github.lechandonga.localeventbrokerjava.web;

public record DeliveryResponse(long offset,
                               String eventKey,
                               String payload,
                               String deliveryToken,
                               long expiresAt,
                               int attempt) {
}
