package com.accor.tech.admin.config;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KafkaSecurityConfig {

    private String jaasConfig;

    private String securityProtocol;

    private String saslMechanism;

    private String sslTrustoreLocation;

    private String sslTrustorePwd;

    private String sslKeyStoreLocation;

    private String sslKeyStorePwd;

    private String sslPrivateKeyPwd;

}
