package learningspark04.mapper;

import java.io.Serializable;
import java.math.BigDecimal;
//import java.text.SimpleDateFormat;
import java.time.LocalDate;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import learningspark04.pojo.House;

public class HouseMapper  implements MapFunction <Row, House>, Serializable {

	private static final long serialVersionUID = 8510144709650460785L;
//	private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-mm-dd");

	@Override
	public House call(Row value) throws Exception {
		House h = new House();
		h.setId(value.getAs("id"));
		h.setAddress(value.getAs("address"));
		h.setSqft(value.getAs("sqft"));
		Double price = value.getAs("price");
		h.setPrice(new BigDecimal(price));
		String dataStr = value.getAs("vacantBy");
		
		// Parse jah eh por padrao yyyy-mm-dd
		h.setVacantBy(LocalDate.parse(dataStr));
		return h;
	}

}
