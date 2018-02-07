/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * This class implements a a `PBKDF2WithHmacSHA1` based password digest creator and validator.
 *
 * Following steps are taken during the creation of the digest:
 *  1. A salt is generated.
 *  2. The password is encoded with this salt.
 *  3. Number of iterations, salt and this password is string encoded and concatenated with ":" as separator.
 *  4. This whole string is again string encoded with base 16.
 *
 *  For validation these steps are reversed to get the password digest from the stored password. The incoming password
 *  is digested with the retrieved iterations and salt. The generated digest is then cross checked against the created digest.
 */
public class StrongPasswordProcessor {

    private final int iterations = 1000;

    /**
     * @param password              The incoming password.
     * @param encryptedPassword     The stored password digest.
     * @return                      true if the password matches, false otehrwise.
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeySpecException
     */
    public boolean checkPassword(String password, String encryptedPassword) throws NoSuchAlgorithmException, InvalidKeySpecException {
        String storedPassword = new String(fromHex(encryptedPassword));
        String[] parts = storedPassword.split(":");
        int iterations = Integer.parseInt(parts[0]);
        byte[] salt = fromHex(parts[1]);
        byte[] hash = fromHex(parts[2]);

        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, hash.length * 8);
        SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        byte[] testHash = skf.generateSecret(spec).getEncoded();

        int diff = hash.length ^ testHash.length;
        for (int i = 0; i < hash.length && i < testHash.length; i++) {
            diff |= hash[i] ^ testHash[i];
        }
        return diff == 0;
    }

    /**
     * @param userPassword The password to be encrypted.
     * @return              The encrypted string digest.
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeySpecException
     */
    public String encryptPassword(String userPassword) throws NoSuchAlgorithmException, InvalidKeySpecException {
        char[] chars = userPassword.toCharArray();
        byte[] salt = getSalt();

        PBEKeySpec spec = new PBEKeySpec(chars, salt, iterations, 64 * 8);
        SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        byte[] hash = skf.generateSecret(spec).getEncoded();
        return toHex((iterations + ":" + toHex(salt) + ":" + toHex(hash)).getBytes());
    }

    private byte[] getSalt() throws NoSuchAlgorithmException {
        SecureRandom sr = SecureRandom.getInstance("SHA1PRNG");
        byte[] salt = new byte[16];
        sr.nextBytes(salt);
        return salt;
    }

    private String toHex(byte[] array) throws NoSuchAlgorithmException {
        BigInteger bi = new BigInteger(1, array);
        String hex = bi.toString(16);
        int paddingLength = (array.length * 2) - hex.length();
        if (paddingLength > 0) {
            return String.format("%0"  +paddingLength + "d", 0) + hex;
        } else {
            return hex;
        }
    }

    private byte[] fromHex(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return bytes;
    }
}