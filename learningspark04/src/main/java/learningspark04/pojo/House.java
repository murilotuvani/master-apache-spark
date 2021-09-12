package learningspark04.pojo;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;

public class House implements Serializable {

	private static final long serialVersionUID = 1246553007711392346L;
	private int id;
	private String address;
	private int sqft;
	private BigDecimal price;
	private LocalDate vacantBy;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public int getSqft() {
		return sqft;
	}

	public void setSqft(int sqft) {
		this.sqft = sqft;
	}

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
	}

	public LocalDate getVacantBy() {
		return vacantBy;
	}

	public void setVacantBy(LocalDate vacantBy) {
		this.vacantBy = vacantBy;
	}

}
