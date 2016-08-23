package com.zendesk.maxwell;

/**
 * Created by davesmelker on 11/9/15.
 */

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxwellEncrypt {

    static final Logger LOGGER = LoggerFactory.getLogger(MaxwellEncrypt.class);



    public static String encrypt( String value) {




        String key =  Maxwell.encryptionKey; //"Tl1yAExxM4ocVd0l";
        String initVector = Maxwell.secertKey; //"RandomInitVector";
        //LOGGER.info( "Encryption Key Is " + key );
        //LOGGER.info( "Encryption Secret Is " + initVector );
        try {
            IvParameterSpec iv = new IvParameterSpec(initVector.getBytes("UTF-8"));
            SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");

            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);

            byte[] encrypted = cipher.doFinal(value.getBytes());
            //System.out.println("encrypted string: "
            //        + Base64.encodeBase64String(encrypted));

            return Base64.encodeBase64String(encrypted);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

}
