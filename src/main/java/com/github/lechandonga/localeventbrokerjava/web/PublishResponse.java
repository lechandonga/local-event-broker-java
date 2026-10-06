package com.github.lechandonga.localeventbrokerjava.web;

public record PublishResponse(String topic, long offset, boolean duplicated) {
}
