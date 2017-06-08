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

#include "EncryptionService.h"
#include "Logger.h"

using namespace Hdfs::Internal;

namespace Hdfs {

EncryptionService::EncryptionService(FileEncryptionInfo *encryptionInfo, KmsService *ks)
{
		
	// 0. init global status
	ERR_load_crypto_strings();
	OpenSSL_add_all_algorithms();
	OPENSSL_config(NULL);

	// 1. create cipher context
	encryptCtx = EVP_CIPHER_CTX_new();		
	decryptCtx = EVP_CIPHER_CTX_new();	
	cipher = NULL;	

	this->encryptionInfo = encryptionInfo;
	this->ks = ks;
}

EncryptionService::EncryptionService(FileEncryptionInfo *encryptionInfo) : EncryptionService(encryptionInfo, NULL)
{
	this->ks = new KmsService(new KmsHttpClient); 
}

std::string EncryptionService::endecInternal(const char * buffer, int64_t size, bool enc)
{
	std::string key = encryptionInfo->getKey();
	std::string iv = encryptionInfo->getIv();
	LOG(INFO, "endecInternal info. key:%s iv:%s buffer:%s size:%ld is_encode:%b", key.c_str(), iv.c_str(), buffer, size, enc);
	// OPENSSL 
	
	// 0. init global status

	
	// 1. create cipher context
	key = ks->getKey(*encryptionInfo);

	// 2. select cipher method
	if (key.length() == 32) {
		cipher = EVP_aes_256_ctr();	
	} else if (key.length() == 16) {
		cipher = EVP_aes_128_ctr();
	} else {
		cipher = EVP_aes_192_ctr();
	}

	// 3. init cipher context with 
		// 3.1 cipher method		based on key length
		// 3.2 encrypted key		from KMS
		// 3.3 IV					from KMS
	int encode = enc ? 1 : 0;
	if (!EVP_CipherInit_ex(encryptCtx, cipher, NULL, (const unsigned char *)key.c_str(), (const unsigned char *)iv.c_str(), encode)) {
		LOG(INFO, "EVP_CipherInit_ex failed");
	}
	LOG(INFO, "EVP_CipherInit_ex successfully");

	// 4. encode/decode buffer within cipher context
	std::string result;
	int len = 0;
	if (!EVP_CipherUpdate(encryptCtx, (unsigned char *)&result[0], &len, (const unsigned char *)buffer, size)) {
		LOG(INFO, "EVP_CipherUpdate failed");

	}
	LOG(INFO, "EVP_CipherUpdate successfully, result:%s, len:%d", result.c_str(), len);

	return result;
}

std::string EncryptionService::encode(const char * buffer, int64_t size)
{
	return endecInternal(buffer, size, true);
}
	
std::string EncryptionService::decode(const char * buffer, int64_t size)
{
	return endecInternal(buffer, size, false);
}

	
}

