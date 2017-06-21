/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "CryptoCodec.h"
#include "Logger.h"

using namespace Hdfs::Internal;

namespace Hdfs {

CryptoCodec::CryptoCodec(FileEncryptionInfo *encryptionInfo, std::shared_ptr<KmsClientProvider> kcp, int32_t bufSize) : encryptionInfo(encryptionInfo), kcp(kcp), bufSize(bufSize)
{
		
	// Init global status
	ERR_load_crypto_strings();
	OpenSSL_add_all_algorithms();
	OPENSSL_config(NULL);

	// Create cipher context
	encryptCtx = EVP_CIPHER_CTX_new();		
	cipher = NULL;	

}

CryptoCodec::~CryptoCodec()
{
	if (encryptCtx) 
		EVP_CIPHER_CTX_free(encryptCtx);
}

std::string CryptoCodec::getDecryptedKeyFromKms()
{
	ptree map = kcp->decryptEncryptedKey(*encryptionInfo);
	std::string key = map.get<std::string>("material");

	int rem = key.length() % 4;
    if (rem) {
        rem = 4 - rem;
        while (rem != 0) {
            key = key + "=";
            rem--;
        }
    }

    std::replace(key.begin(), key.end(), '-', '+');
    std::replace(key.begin(), key.end(), '_', '/');

    LOG(INFO, "material is :%s", key.c_str());
	
	key = kcp->base64Decode(key);
	return key;

	
}

std::string CryptoCodec::endecInternal(const char * buffer, int64_t size, bool enc)
{
	std::string key = encryptionInfo->getKey();
	std::string iv = encryptionInfo->getIv();
	LOG(INFO, "endecInternal info. key:%s iv:%s buffer:%s size:%ld is_encode:%b", key.c_str(), iv.c_str(), buffer, size, enc);
	
	//Get encrypted key from KMS
	key = getDecryptedKeyFromKms();

	// Select cipher method
	if (key.length() == 32) {
		cipher = EVP_aes_256_ctr();	
	} else if (key.length() == 16) {
		cipher = EVP_aes_128_ctr();
	} else {
		cipher = EVP_aes_192_ctr();
	}

	// Init cipher context with cipher method, encrypted key and IV from KMS.
	int encode = enc ? 1 : 0;
	if (!EVP_CipherInit_ex(encryptCtx, cipher, NULL, (const unsigned char *)key.c_str(), (const unsigned char *)iv.c_str(), encode)) {
		LOG(INFO, "EVP_CipherInit_ex failed");
	}
	LOG(INFO, "EVP_CipherInit_ex successfully");
	EVP_CIPHER_CTX_set_padding(encryptCtx, 0);

	// encode/decode buffer within cipher context
	std::string result;
	result.resize(size);
	int offset = 0;
	int remaining = size;
	int len = 0;
	while (remaining > bufSize) {
		if (!EVP_CipherUpdate(encryptCtx, (unsigned char *)&result[offset], &len, (const unsigned char *)buffer+offset, bufSize)) {
			std::string err = ERR_lib_error_string(ERR_get_error());
			THROW(HdfsIOException, "Cannot encrypt AES data %s", err.c_str());
		}
		offset += len;
		remaining -= len;
		LOG(INFO, "EVP_CipherUpdate successfully, result:%s, len:%d", result.c_str(), len);
	}
	if (remaining) {
		if (!EVP_CipherUpdate(encryptCtx, (unsigned char *)&result[offset], &len, (const unsigned char *)buffer+offset, remaining)) {
			std::string err = ERR_lib_error_string(ERR_get_error());
			THROW(HdfsIOException, "Cannot encrypt AES data %s", err.c_str());
		}
	}

	return result;
}

std::string CryptoCodec::encode(const char * buffer, int64_t size)
{
	return endecInternal(buffer, size, true);
}
	
std::string CryptoCodec::decode(const char * buffer, int64_t size)
{
	return endecInternal(buffer, size, false);
}
	
}

