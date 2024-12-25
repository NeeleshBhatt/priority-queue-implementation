package com.example;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Message {
  /** How many times this message has been delivered. */
  private int attempts;

  /** Visible from time */
  private long visibleFrom;

  /** An identifier associated with the act of receiving the message. */
  private String receiptId;

  private String msgBody;

  Message() {

  }

  Message(String msgBody) {
    this.msgBody = msgBody;
  }

  Message(String msgBody, String receiptId) {
    this.msgBody = msgBody;
    this.receiptId = receiptId;
  }

  public String getReceiptId() {
    return this.receiptId;
  }

  protected void setReceiptId(String receiptId) {
    this.receiptId = receiptId;
  }

  protected void setVisibleFrom(long visibleFrom) {
    this.visibleFrom = visibleFrom;
  }

  public long getVisibleFrom() {
    return visibleFrom;
  }

  /*
  public boolean isVisible() {
  	return visibleFrom < System.currentTimeMillis();
  }*/

  public boolean isVisibleAt(long instant) {
    return visibleFrom < instant;
  }

  @JsonProperty("msgBody")
  public String getBody() {
    return msgBody;
  }

  public void setMsgBody(String msgBody) {
    this.msgBody = msgBody;
  }

  protected int getAttempts() {
    return attempts;
  }

  protected void incrementAttempts() {
    this.attempts++;
  }
}
