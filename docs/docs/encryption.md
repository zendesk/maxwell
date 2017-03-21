### Using encryption
***
You can encrypt your data using your own encryption and secret keys from the command-line using a AES/CBC/PKCS5PADDING cipher.
Values are first encrypted and then base64 encoded

option                                        | description
--------------------------------------------- | -----------
--encrypt_data                                | encrypt the data field
--encrypt_all                                 | encrypt the entire payload, takes precedence over encrypt_data if both specified
--encryption_key                              | encryption key to be used
--secret_key                                  | secret key to be used

### Decryption
***
To decrypt your data you must first decode the string from base64 and then apply the cipher to decrypt. The method is provided in RowEncrypt.decrypt(), it looks like this:

```
try {
    IvParameterSpec ivSpec = new IvParameterSpec(initVector.getBytes("UTF-8"));
    SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");

    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
    cipher.init(Cipher.DECRYPT_MODE, skeySpec, ivSpec);

    return new String(cipher.doFinal(Base64.decodeBase64(value.getBytes("UTF-8"))), Charset.forName("UTF-8"));
} catch (Exception ex) {
    ex.printStackTrace();
}
```

### Examples
***

insert into minimal set account_id =1, text_field='hello'

Not encrypted
```
{"database":"shard_1","table":"minimal","type":"insert","ts":1490115785,"xid":153,"commit":true,"data":{"id":1,"account_id":1,"text_field":"hello"}}
```

--encrypt_data=true
--encryption_key=aaaaaaaaaaaaaaaa
--secret_key=RandomInitVector
```
{"database":"shard_1","table":"minimal","type":"insert","ts":1490115722,"xid":153,"commit":true,"data":"WXGOoMWluiiRiH4+T4ugzWQn1VPE4edWOXBTFDfFK9D32QEqdILuWSib9xVzHyTH"}
```

--encrypt_all=true
--encryption_key=aaaaaaaaaaaaaaaa
--secret_key=RandomInitVector
```
O2S/+3/1rDf011nizxhsrSj89HGM7icna+xIcuCk/VaeC80NXjKR57/6AhrkLerc+F5yKKIPxmFwjwPeZ87438WPaNm1f4ffZeEjgQ17KYgQ3v4ohkDH48F+wsXrPyeW1vhkkubfyoHDBGvmv69C7zHudI+5FVuO5lqYaVTMeApk+I+3xib0lr9Peygwi9ufJlCfQk9cYOf37U0hKGbF/A==
```