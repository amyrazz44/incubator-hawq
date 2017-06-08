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
#ifndef _HDFS_LIBHDFS3_CLIENT_KMSHTTPCLIENT_H_
#define _HDFS_LIBHDFS3_CLIENT_KMSHTTPCLIENT_H_

#include <string>
#include <vector>
#include <curl/curl.h>

namespace Hdfs {

class KmsHttpClient {
public:
    KmsHttpClient();

	virtual ~KmsHttpClient();

	virtual std::string post(std::string url, const std::vector<std::string> &headers, std::string body);

	const std::vector<std::string>& getDefaultHeaders() {
		headers.push_back("Content-Type: application/json");
		headers.push_back("Accept: *");
		return headers;
	}

	std::string escape(const std::string data) {
		return curl_easy_escape(curl, data.c_str(), data.length());
	}

private:
	std::vector<std::string> headers;	
	CURL *curl;
	struct curl_slist *list;
	std::string response;
};

}
#endif
