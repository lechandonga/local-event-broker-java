package com.github.lechandonga.localeventbrokerjava.broker;

public record PublishOutcome(long offset, boolean duplicated) {
}
