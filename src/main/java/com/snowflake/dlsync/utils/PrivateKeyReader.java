package com.snowflake.dlsync.utils;

import java.io.FileReader;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.Security;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;


public class PrivateKeyReader {

    // Implement passphrase fetching as you prefer
    private static String getPrivateKeyPassphrase() {
        return System.getenv("PRIVATE_KEY_PASSPHRASE");
    }

    public static PrivateKey get(String filename) throws Exception {
        Security.addProvider(new BouncyCastleProvider());
        PEMParser pemParser = new PEMParser(new FileReader(Paths.get(filename).toFile()));
        Object pemObject = pemParser.readObject();
        PrivateKeyInfo privateKeyInfo;

        if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
            PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
            String passphrase = getPrivateKeyPassphrase();
            InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                .build(passphrase.toCharArray());
            privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
        } else if (pemObject instanceof PrivateKeyInfo) {
            privateKeyInfo = (PrivateKeyInfo) pemObject;
        } else {
            throw new IllegalArgumentException("Unsupported key format");
        }

        pemParser.close();
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter()
            .setProvider(BouncyCastleProvider.PROVIDER_NAME);
        return converter.getPrivateKey(privateKeyInfo);
    }
}
