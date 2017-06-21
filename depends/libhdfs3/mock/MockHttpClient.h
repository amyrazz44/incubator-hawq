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
#ifndef _HDFS_LIBHDFS3_MOCK_HTTPCLIENT_H_
#define _HDFS_LIBHDFS3_MOCK_HTTPCLIENT_H_

#include "gmock/gmock.h"

#include "client/HttpClient.h"
#include "client/KmsClientProvider.h"
#include <boost/property_tree/ptree.hpp>

using boost::property_tree::ptree;

class MockHttpClient: public Hdfs::HttpClient {
public:
  MOCK_METHOD1(setURL, void(const std::string &url));
  MOCK_METHOD1(setHeaders, void(const std::vector<std::string> &headers));
  MOCK_METHOD1(setBody, void(const std::string &body));
  MOCK_METHOD1(setResponseSuccessCode, void(const long response_code_ok));
  MOCK_METHOD0(init, void());
  MOCK_METHOD0(destroy, void());
  MOCK_METHOD1(escape, std::string(const std::string &data));
  MOCK_METHOD0(errorString, std::string());
  MOCK_METHOD0(HttpPost, std::string());
  MOCK_METHOD0(HttpDelete, std::string());
  MOCK_METHOD0(HttpPut, std::string());
  MOCK_METHOD0(HttpGet, std::string());

  std::string getPostResult(FileEncryptionInfo &encryptionInfo) {
	ptree map;
	map.put("name", encryptionInfo.getKeyName());
	map.put("iv", encryptionInfo.getIv());
	map.put("material", encryptionInfo.getKey());

	std::string json = KmsClientProvider::toJson(map);
	return json;
  }


};

#endif /* _HDFS_LIBHDFS3_MOCK_HTTPCLIENT_H_ */
