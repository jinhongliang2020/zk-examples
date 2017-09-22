import org.hong.zk.utils.EncryptUtil;
import org.junit.Test;

/**
 * Created by Administrator on 2017/9/21.
 */
public class EncryptUtilTest {

    @Test
    public void test() {
        //sha1 加密
        String pwd = EncryptUtil.SHA1("123456");
        System.out.println(pwd);

        //base64 编码
        String encodePwd= EncryptUtil.encodeBase64(pwd);
        System.out.println(encodePwd.equals("N2M0YThkMDljYTM3NjJhZjYxZTU5NTIwOTQzZGMyNjQ5NGY4OTQxYg=="));

    }

}
