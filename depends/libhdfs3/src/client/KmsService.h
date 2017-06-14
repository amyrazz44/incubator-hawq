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
#ifndef _HDFS_LIBHDFS3_CLIENT_KMSSERVICE_H_
#define _HDFS_LIBHDFS3_CLIENT_KMSSERVICE_H_

#include <string>
#include <gsasl.h>

#include "openssl/conf.h"
#include "openssl/evp.h"
#include "openssl/err.h"
#include "FileEncryptionInfo.h"
#include "HttpClient.h"
#include <vector>

namespace Hdfs {

class KmsService {
public:
    KmsService(HttpClient *hc);

	virtual ~KmsService(){
	}

	std::string getKey(FileEncryptionInfo &encryptionInfo);

	std::string getKmsUrl(const std::string &key);
	const std::vector<std::string> getKmsHeaders();
	std::string getBody(const std::string &name, const std::string &iv, const std::string &material);

private:
	//std::string toJson(ptree &data);
	//ptree		fromJson(const std::string &data);
	std::string	base64Encode(const std::string &data);
	std::string	base64Decode(const std::string &data);

	FileEncryptionInfo *encryptionInfo;
	HttpClient 		*hc;
};

}
#endif
