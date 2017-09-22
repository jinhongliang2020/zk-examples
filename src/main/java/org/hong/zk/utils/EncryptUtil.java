package org.hong.zk.utils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;

import java.security.DigestException;

/**
 * 加密编码工具类.
 * Created by Administrator on 2017/9/21.
 */
public class EncryptUtil {

    /**
     * SHA1 安全加密算法
     * @return
     * @throws DigestException
     */
    public static String SHA1(String pwd){
       return DigestUtils.sha1Hex(pwd);
    }
    /**
     * base64 编码
     * @return
     */
    public static String encodeBase64(String code){
        return Base64.encodeBase64String(code.getBytes());
    }

}
