package pojo;

import java.sql.Date;

public class NSEStock {

	private Date date;
	private float openPrice;
	private float highestPrice;
	private float lowestPrice;
	private float closingPrice;
	private float quantity;
	private float turnover;

	public float getOpenPrice() {
		return openPrice;
	}

	public float getHighestPrice() {
		return highestPrice;
	}

	public float getLowestPrice() {
		return lowestPrice;
	}

	public float getClosingPrice() {
		return closingPrice;
	}

	public float getQuantity() {
		return quantity;
	}

	public float getTurnover() {
		return turnover;
	}

	public Date getDate() {
		return date;
	}

}
