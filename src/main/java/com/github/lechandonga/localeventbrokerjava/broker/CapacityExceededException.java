package com.github.lechandonga.localeventbrokerjava.broker;

/** 主题达到容量上限时抛出，代表明确的背压语义：本次发布未被接受，调用方应稍后重试。 */
public class CapacityExceededException extends RuntimeException {
    public CapacityExceededException(String message) {
        super(message);
    }
}
