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
#ifndef _HDFS_LIBHDFS3_CLIENT_KMSCLIENTPROVIDER_H_
#define _HDFS_LIBHDFS3_CLIENT_KMSCLIENTPROVIDER_H_

#include <string>
#include <gsasl.h>

#include "openssl/conf.h"
#include "openssl/evp.h"
#include "openssl/err.h"
#include "FileEncryptionInfo.h"
#include "HttpClient.h"
#include <vector>
#include "common/SessionConfig.h"
#include "rpc/RpcAuth.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

using boost::property_tree::ptree;
using boost::property_tree::read_json;
using boost::property_tree::write_json;
using namespace Hdfs::Internal;

namespace Hdfs {

class KmsClientProvider {
public:
	KmsClientProvider(std::shared_ptr<RpcAuth> auth, std::shared_ptr<SessionConfig> conf);

	virtual ~KmsClientProvider(){
	}

	void setHttpClient(std::shared_ptr<HttpClient> hc);
	
	void createKey(const std::string &keyName, const std::string &cipher, const int length, const std::string &material, const std::string &description);

	ptree getKeyMetadata(const FileEncryptionInfo &encryptionInfo);

	void deleteKey(const FileEncryptionInfo &encryptionInfo);

	ptree decryptEncryptedKey(const FileEncryptionInfo &encryptionInfo);

	std::string	base64Encode(const std::string &data);

	std::string	base64Decode(const std::string &data);

private:
	static std::string  toJson(ptree &data);
	static ptree		fromJson(const std::string &data);
	std::string parseKmsUrl(std::shared_ptr<SessionConfig> conf);
	std::string buildKmsUrl(const std::string url, const std::string urlSuffix);

	std::shared_ptr<HttpClient> 	hc;
	std::string						url;

	std::shared_ptr<RpcAuth> 		auth;
	AuthMethod						method;
	std::shared_ptr<SessionConfig> 	conf;
	
};

}
#endif
