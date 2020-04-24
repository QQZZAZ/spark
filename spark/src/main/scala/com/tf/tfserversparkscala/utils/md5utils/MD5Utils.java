package com.tf.tfserversparkscala.utils.md5utils;


import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;

/**
 * MD5工具类
 */
public class MD5Utils {

    /**
     * 仅支持数字和标点符号的加密和解密
     *
     * @param value
     * @param secret
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String secret(String value, char secret) throws UnsupportedEncodingException {
        byte[] bt = value.getBytes();//将需要加密的内容转换为字节数组

        for (int i = 0; i < bt.length; i++) {
            bt[i] = (byte) (bt[i] ^ (int) secret); //通过异或运算进行加密
        }
        String newresult = new String(bt, 0, bt.length);//将加密后的字符串保存到 newresult 变量中
        return newresult;

    }

    protected static char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6',
            '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    protected static MessageDigest messagedigest = null;

    static {
        try {
            messagedigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException nsaex) {
            nsaex.printStackTrace();
        }
    }

    public static String getMD5String(String input) {
        byte[] inputByteArray = input.getBytes();
        messagedigest.update(inputByteArray);
        return bufferToHex(messagedigest.digest());
    }

    private static String bufferToHex(byte bytes[]) {
        return bufferToHex(bytes, 0, bytes.length);
    }

    private static String bufferToHex(byte bytes[], int m, int n) {
        StringBuffer stringbuffer = new StringBuffer(2 * n);
        int k = m + n;
        for (int l = m; l < k; l++) {
            appendHexPair(bytes[l], stringbuffer);
        }
        return stringbuffer.toString();
    }

    private static void appendHexPair(byte bt, StringBuffer stringbuffer) {
        char c0 = hexDigits[(bt & 0xf0) >> 4];
        char c1 = hexDigits[bt & 0xf];
        stringbuffer.append(c0);
        stringbuffer.append(c1);
    }

    public static void main(String[] args) throws Exception {
        Scanner scan = new Scanner(System.in);
        char secret = '8';//加密文字符
        System.out.println("请输入您想加密的内容：");
        String pass = scan.next();
        System.out.println("原字符串内容：" + pass);
        String secretResult = secret(pass, secret);
        System.out.println("加密后的内容：" + secretResult);
        String uncryptResult = secret(secretResult, secret);
        System.out.println("解密后的内容：" + uncryptResult);
    }
}
