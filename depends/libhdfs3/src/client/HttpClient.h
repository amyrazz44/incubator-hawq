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

namespace Hdfs {

class HttpClient {
public:
	HttpClient();

	HttpClient(std::string url, std::vector<std::string> headers, std::string body);

	virtual ~HttpClient();

	virtual void setURL(const std::string &url);	

	virtual void setHeaders(const std::vector<std::string> &headers);
	
	virtual void setBody(const std::string &body);	
	
	virtual void setResponseSuccessCode(const long response_code_ok);

	virtual void init();
	
	virtual void destroy();	
	
	virtual std::string HttpPost();
	
	virtual std::string HttpDelete();
	
	virtual std::string HttpPut();
	
	virtual std::string HttpGet();

	virtual std::string escape(const std::string &data);

	virtual std::string errorString();

private:
 	std::string HttpInternal(int method);
	static size_t CurlWriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp); 
	static bool initialized;
	CURLcode res;
	std::string url;
	std::vector<std::string> headers;
	std::string body;
	long response_code_ok;	
	CURL *curl;
	struct curl_slist *list;
	std::string response;
	char errbuf[CURL_ERROR_SIZE] = {0};
};

}
#endif
