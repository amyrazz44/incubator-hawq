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

#include "KmsHttpClient.h"
#include "Logger.h"

using namespace Hdfs::Internal;

namespace Hdfs {

size_t CurlWriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
      size_t realsize = size * nmemb;
      ((std::string *)userp)->append((const char *)contents, realsize);

	  LOG(INFO, "http response : %s", ((std::string *)userp)->c_str());
      return realsize;
}

KmsHttpClient::KmsHttpClient()
{
	CURLcode ret = curl_global_init(CURL_GLOBAL_ALL);
	curl = curl_easy_init();
    
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&response);

	list = NULL;
}

KmsHttpClient::~KmsHttpClient()
{
	curl_easy_cleanup(curl);
	curl_slist_free_all(list);
}

	
std::string KmsHttpClient::post(std::string url, const std::vector<std::string> &headers, std::string body) {

	LOG(INFO, "kms url:%s", url.c_str());

	for (std::string header : headers) {
		LOG(INFO, "kms header:%s", header.c_str());
		list = curl_slist_append(list, header.c_str());
	}
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);
	
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

	curl_easy_setopt(curl, CURLOPT_COPYPOSTFIELDS, body.c_str());
	
	curl_easy_perform(curl);

	return response;


}

	
}

