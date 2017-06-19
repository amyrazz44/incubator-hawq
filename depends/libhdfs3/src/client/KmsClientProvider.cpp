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

#include "KmsClientProvider.h"
#include "Logger.h"
#include <gsasl.h>
#include <map>
using namespace Hdfs::Internal;

namespace Hdfs {

std::string KmsClientProvider::toJson(ptree &data)
{
	std::ostringstream buf;
	write_json(buf, data, false);
	std::string json = buf.str();
	return json;
}

ptree KmsClientProvider::fromJson(const std::string &data)
{
	ptree pt2;
	std::istringstream is(data);
	read_json(is, pt2);
	return pt2;
}

KmsClientProvider::KmsClientProvider(std::shared_ptr<RpcAuth> rpcAuth, std::shared_ptr<SessionConfig> config) : auth(rpcAuth), conf(config) , hc(new HttpClient())
{
	url = parseKmsUrl(conf);
	method = RpcAuth::ParseMethod(conf->getKmsMethod());
}

void KmsClientProvider::setHttpClient(std::shared_ptr<HttpClient> hc)
{
	this->hc = hc;
}

std::string KmsClientProvider::parseKmsUrl(std::shared_ptr<SessionConfig> conf) 
{
	std::string start = "kms://";
    std::string http = "http@";
    std::string https = "https@";
	std::string urlParse = conf->getKmsUrl(); 
    if (urlParse.compare(0, start.length(), start) == 0) {
        start = urlParse.substr(start.length());
        if (start.compare(0, http.length(), http) == 0) {
            return "http://" + start.substr(http.length());
        }
        else if (start.compare(0, https.length(), https) == 0) {
            return "https://" + start.substr(https.length());
        }
        else
            THROW(HdfsIOException, "Bad KMS provider URL: %s", urlParse.c_str());
    }
    else
        THROW(HdfsIOException, "Bad KMS provider URL: %s", urlParse.c_str());

}

std::string KmsClientProvider::buildKmsUrl(const std::string url, const std::string urlSuffix)
{
		std::string baseUrl = url;
        baseUrl = url + "/v1/" + urlSuffix;
		std::size_t found = urlSuffix.find('?');

        if (method == AuthMethod::KERBEROS) {
            return baseUrl;
        } else if (method == AuthMethod::SIMPLE) {
            std::string user = auth->getUser().getRealUser();
            if (user.length() == 0)
                user = auth->getUser().getKrbName();
			if (found != std::string::npos)
            	return baseUrl + "&user.name=" + user;
			else
				return baseUrl + "?user.name=" + user;
        } else {
            return baseUrl;
        }	
}

void KmsClientProvider::createKey(const std::string &keyName, const std::string &cipher, const int length, const std::string &material, const std::string &description)
{
	std::string urlSuffix = "keys";
	url = buildKmsUrl(url, urlSuffix);

	std::vector<std::string> headers;
	headers.push_back("Content-Type: application/json");
 	headers.push_back("Accept: *");

	ptree map;
    map.put("name", keyName);
    map.put("cipher", cipher);
	//map.put("length", length);
    //map.put("material", base64Encode(material));
	map.put("description", description);
    std::string body = toJson(map);	
	LOG(INFO, "create key body is %s", body.c_str());
	hc->init();
	hc->setURL(url);
	hc->setHeaders(headers);
	hc->setBody(body);
	hc->setResponseSuccessCode(201);
	std::string response = hc->HttpPost();
	hc->destroy();
		
} 

ptree KmsClientProvider::getKeyMetadata(const FileEncryptionInfo &encryptionInfo)
{
	std::string urlSuffix = "key/" + hc->escape(encryptionInfo.getKeyName()) + "/_metadata";
	url = buildKmsUrl(url, urlSuffix);
	
	hc->init();
	hc->setURL(url);
	hc->setResponseSuccessCode(200);
	std::string response = hc->HttpGet();
	hc->destroy();
	ptree map = fromJson(response);
	return map;

}

void KmsClientProvider::deleteKey(const FileEncryptionInfo &encryptionInfo)
{
	std::string urlSuffix = "key/" + hc->escape(encryptionInfo.getKeyName());
	url = buildKmsUrl(url, urlSuffix);
	
	hc->init();
    hc->setURL(url);
    hc->setResponseSuccessCode(200);
	std::string response = hc->HttpDelete();
    hc->destroy();
}

ptree KmsClientProvider::decryptEncryptedKey(const FileEncryptionInfo &encryptionInfo)
{
	// prepare HttpClient url
	std::string urlSuffix = "keyversion/" + hc->escape(encryptionInfo.getEzKeyVersionName()) + "/_eek?eek_op=decrypt";
	url = buildKmsUrl(url, urlSuffix);
	LOG(INFO, "http url is : %s", url.c_str());
	// prepare HttpClient headers
	std::vector<std::string> headers;
	headers.push_back("Content-Type: application/json");
 	headers.push_back("Accept: *");
	for(std::string header : headers) {
		LOG(INFO, "header is %s", header.c_str());
	}
	// prepare HttpClient body in json format
	ptree map;
    map.put("name", encryptionInfo.getKeyName());
    map.put("iv", base64Encode(encryptionInfo.getIv()));
    map.put("material", base64Encode(encryptionInfo.getKey()));
    std::string body = toJson(map);	

	LOG(INFO, "http body : %s", body.c_str());

	// call HttpClient to get response
	hc->init();
	hc->setURL(url);
	hc->setHeaders(headers);
	hc->setBody(body);
	hc->setResponseSuccessCode(200);
	std::string response = hc->HttpPost();
	hc->destroy();
	// convert json to map
	map = fromJson(response);

	return map;
	/*
	std::string material = map.get<std::string>("material");

	int rem = material.length() % 4;
	if (rem) {
		rem = 4 - rem;
		while (rem != 0) {
			material = material + "=";
			rem--;
		}
	}

	std::replace(material.begin(), material.end(), '-', '+');
	std::replace(material.begin(), material.end(), '_', '/');

	LOG(INFO, "material is :%s", material.c_str());	

	//7. return key
	return base64Decode(material);
	*/
}

std::string	KmsClientProvider::base64Encode(const std::string &data)
{
	char * buffer;
	size_t len;
	gsasl_base64_to(data.c_str(), data.size(), &buffer, &len);
	std::string result;
	result.assign(buffer, len);
	free(buffer);
	return result;	
}

std::string	KmsClientProvider::base64Decode(const std::string &data)
{
	char * buffer;
	size_t len;
	gsasl_base64_from(data.c_str(), data.size(), &buffer, &len);
	std::string result;
	result.assign(buffer, len);
	free(buffer);
	return result;	
}

}

