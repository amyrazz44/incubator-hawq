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
#ifndef _HDFS_LIBHDFS3_CLIENT_HTTPCLIENT_H_
#define _HDFS_LIBHDFS3_CLIENT_HTTPCLIENT_H_

#include <string>
#include <vector>
#include <curl/curl.h>
#include "Exception.h"
#include "ExceptionInternal.h"

typedef enum httpMethod {
    E_GET = 0,
	E_POST = 1,
	E_DELETE = 2,
	E_PUT = 3
} httpMethod;

namespace Hdfs {

class HttpClient {
public:
	HttpClient();

	HttpClient(const std::string &url);

	virtual ~HttpClient();

	void setURL(const std::string &url);	

	void setHeaders(const std::vector<std::string> &headers);
	
	void setBody(const std::string &body);	
	
	void setResponseRetryTime(int response_retry_times);	
	
	void setCurlTimeout(int64_t curl_timeout);	
	
	void setExpectedResponseCode(int64_t response_code_ok);

	void init();
	
	void destroy();	
	
	virtual std::string post();
	
	virtual std::string del();
	
	virtual std::string put();
	
	virtual std::string get();

	std::string escape(const std::string &data);

	std::string errorString();

private:
 	std::string httpCommon(httpMethod method);
	static size_t CurlWriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp); 
	static bool initialized;
	CURLcode res;
	std::string url;
	std::vector<std::string> headers;
	std::string body;
	int64_t response_code_ok;
	int response_retry_times;
	int64_t curl_timeout;	
	CURL *curl;
	struct curl_slist *list;
	std::string response;
	char errbuf[CURL_ERROR_SIZE] = {0};
};

}
#endif
