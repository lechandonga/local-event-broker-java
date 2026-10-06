package com.github.lechandonga.localeventbrokerjava.web;

public class AckRequest {
    private long offset;
    /** 拉取时返回的投递令牌，用于防止过期/重复确认。 */
    private String deliveryToken;
    /** NACK 时可选的业务错误信息。 */
    private String error;

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
    public String getDeliveryToken() { return deliveryToken; }
    public void setDeliveryToken(String deliveryToken) { this.deliveryToken = deliveryToken; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
}
