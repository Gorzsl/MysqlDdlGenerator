import com.gorzsl.MysqlDdlGenerator.MysqlDdlGenerator;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author YangWeiDong
 * @date 2022年06月14日 16:52
 */
@Ignore
public class MysqlDdlGeneratorTest {
    @Test
    public void test() {
        String ip = "localhost";
        String port = "3306";
        String schema = "schema";
        String username = "root";
        String password = "123456";
        String sql = "select t1.id, t1.code, t1.status, t1.remark, t2.prod_code, t2.num, t3.price, t2.num*t3.price amount\n" +
                "        from t_main t1\n" +
                "        left join t_detail t2\n" +
                "        on t1.id = t2.main_id\n" +
                "        left join t_product t3\n" +
                "        on t2.prod_code = t3.prod_code";
        MysqlDdlGenerator generator = new MysqlDdlGenerator(ip, port, schema, username, password);
        System.out.println(generator.getDdl(sql));
    }
}
