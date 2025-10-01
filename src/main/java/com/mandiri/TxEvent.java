package com.mandiri;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TxEvent implements Serializable{
	
	    private static final long serialVersionUID = 1L;
		private long sequence;
		private String accountNumber;
		private String trxType;
		private java.math.BigDecimal amount;
		private String currency;
		private java.math.BigDecimal runningBalance;
		private String cif; // will be enriched
		private String ingestTime;
		private String trxDate;
		
		
		
		public String getTrxDate() {
			return trxDate;
		}
		public void setTrxDate(String trxDate) {
			this.trxDate = trxDate;
		}
		public long getSequence() {
			return sequence;
		}
		public void setSequence(long sequence) {
			this.sequence = sequence;
		}
		public String getAccountNumber() {
			return accountNumber;
		}
		public void setAccountNumber(String accountNumber) {
			this.accountNumber = accountNumber;
		}
		public String getTrxType() {
			return trxType;
		}
		public void setTrxType(String trxType) {
			this.trxType = trxType;
		}
		public java.math.BigDecimal getAmount() {
			return amount;
		}
		public void setAmount(java.math.BigDecimal amount) {
			this.amount = amount;
		}
		public String getCurrency() {
			return currency;
		}
		public void setCurrency(String currency) {
			this.currency = currency;
		}
		public java.math.BigDecimal getRunningBalance() {
			return runningBalance;
		}
		public void setRunningBalance(java.math.BigDecimal runningBalance) {
			this.runningBalance = runningBalance;
		}
		public String getCif() {
			return cif;
		}
		public void setCif(String cif) {
			this.cif = cif;
		}
		public String getIngestTime() {
			return ingestTime;
		}
		public void setIngestTime(String ingestTime) {
			this.ingestTime = ingestTime;
		}
		@Override
		public String toString() {
			return "TxEvent [sequence=" + sequence + ", accountNumber=" + accountNumber + ", trxType=" + trxType
					+ ", amount=" + amount + ", currency=" + currency + ", runningBalance=" + runningBalance + ", cif="
					+ cif + ", ingestTime=" + ingestTime + "]";
		}

	    

}
