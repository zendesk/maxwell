### Using encryption
***
You can encrypt your data using your own encryption and secret keys from the command-line using a AES/CBC/PKCS5PADDING cipher.
Values are first encrypted and then base64 encoded, an initialization vector is randomly generated and put into the final message

option                                        | description
--------------------------------------------- | -----------
--encrypt_data                                | encrypt the data field
--encrypt_all                                 | encrypt the entire payload, takes precedence over encrypt_data if both specified
--secret_key                                  | secret key to be used

### Decryption
***
To decrypt your data you must first decode the string from base64 and then apply the cipher to decrypt. The method is provided in RowEncrypt.decrypt(), it looks like this:

```
try {
    IvParameterSpec ivSpec = new IvParameterSpec(Base64.decodeBase64(initVector.getBytes("ASCII")));
    SecretKeySpec skeySpec = new SecretKeySpec(secretKey.getBytes("ASCII"), "AES");

    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
    cipher.init(Cipher.DECRYPT_MODE, skeySpec, ivSpec);

    return new String(cipher.doFinal(Base64.decodeBase64(value.getBytes("ASCII"))), Charset.forName("ASCII"));
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
--secret_key=aaaaaaaaaaaaaaaa
```
{database=shard_1, table=minimal, type=insert, ts=1500580778, xid=161, commit=true, data=qZwv1144qaT+k7vWmOsPN6JhZea80wUI0BJOF7C3IKGOtSSIXW0KBf8ZidWhjr7w, init_vector=KSYvEUMw2ljYKC/WAQ/yMA==}
```

--encrypt_all=true
--secret_key=aaaaaaaaaaaaaaaa
```
{data=z4ZfGsKVOLpX35yEmkp0awczCN2+Kr4WoW1asuHhswG3RJzHWj6hS2WCWeRsM0HkuAru8koZy0IANaQvaGvT6DSal/U5OHyKNc6E7IHyMzaK4aJJ82CReG4vdaZynr04ZmReW76Ia/GiQsTe8NCDlz5NtTmKjZf5pT7Q5JxXwU20Zd5NRNelj0mi58ozmi8yAhv9hCRrVIejSaL7uHcX+w==, init_vector=xXTUkIHPsGmRo99LNYALJA==}
```