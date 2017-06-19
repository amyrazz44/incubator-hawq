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

#include "HttpClient.h"
#include "Logger.h"

using namespace Hdfs::Internal;

namespace Hdfs {

#define CURL_SETOPT(handle, option, optarg, fmt, ...) \
    res = curl_easy_setopt(handle, option, optarg); \
    if (res != CURLE_OK) { \
        THROW(HdfsIOException, fmt, ##__VA_ARGS__); \
    }

#define CURL_SETOPT_ERROR1(handle, option, optarg, fmt) \
    CURL_SETOPT(handle, option, optarg, fmt, curl_easy_strerror(res));

#define CURL_SETOPT_ERROR2(handle, option, optarg, fmt) \
    CURL_SETOPT(handle, option, optarg, fmt, curl_easy_strerror(res), \
        errorString().c_str())

#define CURL_PERFORM(handle, fmt) \
    res = curl_easy_perform(handle); \
    if (res != CURLE_OK) { \
        THROW(HdfsIOException, fmt, curl_easy_strerror(res), errorString().c_str()); \
    }


#define CURL_GETOPT_ERROR2(handle, option, optarg, fmt) \
    res = curl_easy_getinfo(handle, option, optarg); \
    if (res != CURLE_OK) { \
        THROW(HdfsIOException, fmt, curl_easy_strerror(res), errorString().c_str()); \
    }

#define CURL_GET_RESPONSE(handle, code, fmt) \
    CURL_GETOPT_ERROR2(handle, CURLINFO_RESPONSE_CODE, code, fmt);



HttpClient::HttpClient() : curl(NULL), list(NULL) 
{			
}

HttpClient::HttpClient(std::string url, std::vector<std::string> headers, std::string body) {
	curl = NULL;
	list = NULL;
	this->url = url;
	for (std::string header : headers) {
		list = curl_slist_append(list, header.c_str());
	}	
	if (!list) {
		THROW(HdfsIOException, "Cannot add header.");
	}
	this->body = body;
}

HttpClient::~HttpClient()
{
	curl = NULL;
	list = NULL;
	errbuf[CURL_ERROR_SIZE] = {0};
}

std::string HttpClient::errorString() {
	if (strlen(errbuf) == 0)
		return "";
	return errbuf;
}

size_t HttpClient::CurlWriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
      size_t realsize = size * nmemb;
      ((std::string *)userp)->append((const char *)contents, realsize);

	  LOG(INFO, "http response : %s", ((std::string *)userp)->c_str());
      return realsize;
}

void HttpClient::init() {
	if (!initialized)
	{
		CURLcode ret = curl_global_init(CURL_GLOBAL_ALL);
		initialized = true;
		if (ret) {
			THROW(HdfsIOException, "Cannot initialize curl client for KMS");
		}
	}

	curl = curl_easy_init();
	if (!curl) {
		THROW(HdfsIOException, "Cannot initialize curl handle for KMS");
	}
	
    CURL_SETOPT_ERROR1(curl, CURLOPT_ERRORBUFFER, errbuf,
        "Cannot initialize curl error buffer for KMS: %s");

    errbuf[0] = 0;

    CURL_SETOPT_ERROR2(curl, CURLOPT_NOPROGRESS, 1,
        "Cannot initialize no progress in HttpClient: %s: %s");

    CURL_SETOPT_ERROR2(curl, CURLOPT_VERBOSE, 0,
        "Cannot initialize no verbose in HttpClient: %s: %s");

    CURL_SETOPT_ERROR2(curl, CURLOPT_COOKIEFILE, "",
        "Cannot initialize cookie behavior in HttpClient: %s: %s");

    CURL_SETOPT_ERROR2(curl, CURLOPT_HTTPHEADER, list,
        "Cannot initialize headers in HttpClient: %s: %s");

    CURL_SETOPT_ERROR2(curl, CURLOPT_WRITEFUNCTION, HttpClient::CurlWriteMemoryCallback,
        "Cannot initialize body reader in HttpClient: %s: %s");

    CURL_SETOPT_ERROR2(curl, CURLOPT_WRITEDATA, (void *)&response,
        "Cannot initialize body reader data in HttpClient: %s: %s");

    /* some servers don't like requests that are made without a user-agent
        field, so we provide one */
    CURL_SETOPT_ERROR2(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0",
        "Cannot initialize user agent for KMS: %s: %s");
	list = NULL;

}

void HttpClient::destroy() {
	if (curl) {
		curl_easy_cleanup(curl);
	}
	if (list) {
		curl_slist_free_all(list);
	}

}

void HttpClient::setURL(const std::string &url) {
	this->url = url;
}

void HttpClient::setHeaders(const std::vector<std::string> &headers) {
	this->headers = headers;
	for (std::string header : headers) {
        list = curl_slist_append(list, header.c_str());
    }
    if (!list) {
        THROW(HdfsIOException, "Cannot add header in HttpClient.");
    }
}

void HttpClient::setBody(const std::string &body) {
	this->body = body;
}

void HttpClient::setResponseSuccessCode(const long response_code_ok) {
	this->response_code_ok = response_code_ok;
}

std::string HttpClient::HttpInternal(int method) {
	long response_code;

	LOG(INFO, "http url is : %s", url.c_str());
	CURL_SETOPT_ERROR2(curl, CURLOPT_HTTPHEADER, list,
                "Cannot initialize headers in HttpClient: %s: %s");

	CURL_SETOPT_ERROR2(curl, CURLOPT_URL, url.c_str(),
            "Cannot initialize url in HttpClient: %s: %s");

	switch(method) {
		case 0:
			break;
		case 1:
			CURL_SETOPT_ERROR2(curl, CURLOPT_COPYPOSTFIELDS, body.c_str(),
                "Cannot initialize post data in HttpClient: %s: %s");
			break;
		case 2:
			CURL_SETOPT_ERROR2(curl, CURLOPT_CUSTOMREQUEST, "DELETE",
                "Cannot initialize set customer request in HttpClient: %s: %s");
			break;
		case 3:
			CURL_SETOPT_ERROR2(curl, CURLOPT_CUSTOMREQUEST, "PUT",
                "Cannot initialize set customer request in HttpClient: %s: %s");
			
			CURL_SETOPT_ERROR2(curl, CURLOPT_COPYPOSTFIELDS, body.c_str(),
                "Cannot initialize post data in HttpClient: %s: %s");
			break;
	}

	CURL_PERFORM(curl, "Could not send request in HttpClient: %s %s");
	
	CURL_GET_RESPONSE(curl, &response_code,
                "Cannot get response code in HttpClient: %s: %s");
	if (response_code != response_code_ok) {
		response = "";
		CURL_PERFORM(curl, "Could not send request in HttpClient: %s %s");
	}
	
	CURL_GET_RESPONSE(curl, &response_code,
                "Cannot get response code in HttpClient: %s: %s");

	if(response_code != response_code_ok) {
		THROW(HdfsIOException, "Got invalid response from HttpClient: %d", (int)response_code);
	}

	return response;
}

std::string HttpClient::HttpGet() {
	return HttpInternal(0);
}

std::string HttpClient::HttpPost() {
	return HttpInternal(1);
}

std::string HttpClient::HttpDelete() {
	return HttpInternal(2);
}

std::string HttpClient::HttpPut() {
	return HttpInternal(3);
}

	
std::string HttpClient::escape(const std::string &data) {
	return curl_easy_escape(curl, data.c_str(), data.length());
}

}

bool Hdfs::HttpClient::initialized = false;

