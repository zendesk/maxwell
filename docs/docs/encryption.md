### Using encryption
***
When encryption is enabled, maxwell will encrypt messages using a AES/CBC/PKCS5PADDING cipher with your own encryption key.
Values are first encrypted and then base64 encoded, an initialization vector is randomly generated and put into the final message

### Decryption
***
To decrypt your data you must first decode the string from base64 and then apply the cipher to decrypt. A sample implementation is provided in RowEncrypt.decrypt().

### Examples
***

insert into minimal set account_id =1, text_field='hello'

`encrypt=none` (unencrypted):

```
{"database":"shard_1","table":"minimal","type":"insert","ts":1490115785,"xid":153,"commit":true,"data":{"id":1,"account_id":1,"text_field":"hello"}}
```

`encrypt=data`:

```
{"database":"shard_1","table":"minimal","type":"insert","ts":1504585129,"xid":161,"commit":true,"encrypted":{"iv":"lqiXoTdz6jed3XgJPpa7EQ==","bytes":"1soc4leskiIm6yuT2D49VA3AYVKCvN+0wh+8d1iwSZETK7N2pG4HDbqnVpJUUCOaKjpcPlP7Sc7Z3SPhGD5JeA=="}}
```

`encrypt=all`:

```
{"encrypted":{"bytes":"iZssjWfzS0NlqIj82ddpvoQeKSx4D3GIPSCgjdkpgQlCWzN2p3VVZOn3Oj1x4w+a6dVhoFmllWxBK6aAkdVK9t6Vt1+um6lWwSeXNQIL/RbknW5Q8I9emm5bC1Dd1LftBuX/1Uw0wjbsq8Qt3HErvmmiIMe2S27EIWshvBnmw9MibryjLD0brvIbFFxwDuSQuVA4OFyV9TN32N/ZXiBwIA==","iv":"XXs6AePsXJWAAIrKyLlR0g=="}}
```
