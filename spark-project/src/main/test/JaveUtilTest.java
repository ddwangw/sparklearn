import java.util.Date;

import com.ibeifeng.sparkproject.util.DateUtils;

public class JaveUtilTest {

	public static void main(String[] args) {
		Date one = DateUtils.parseTime("2017/4/9 15:29:36");
		Date two = DateUtils.parseTime("2017/4/9 15:29:34");
		System.out.println(two.getTime()-one.getTime());
	}

}
