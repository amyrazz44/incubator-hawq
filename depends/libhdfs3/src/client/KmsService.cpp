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

#include "KmsService.h"
#include "Logger.h"
#include <gsasl.h>
#include <map>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

using boost::property_tree::ptree;
using boost::property_tree::read_json;
using boost::property_tree::write_json;


using namespace Hdfs::Internal;

namespace Hdfs {

static std::string toJson(ptree &data)
{
	std::ostringstream buf;
	write_json(buf, data, false);
	std::string json = buf.str();
	return json;
}

static ptree fromJson(const std::string &data)
{
	ptree pt2;
	std::istringstream is(data);
	read_json(is, pt2);
	return pt2;
}




KmsService::KmsService(HttpClient *hc) 
{
	this->hc = hc;
}



std::string KmsService::getKey(FileEncryptionInfo &encryptionInfo)
{
	
	// 0. get key, iv ,version, keyname from FileEncryptionInfo

	// 1. encode key, iv to base64

	// 2. convert key, iv, keyname to json 
	
	// 3. url , headers, body preparation
	std::string url = getKmsUrl(encryptionInfo.getEzKeyVersionName());
	LOG(INFO, "http url is : %s", url.c_str());
	std::vector<std::string> headers = getKmsHeaders();
	for(std::string header : headers) {
		LOG(INFO, "header is %s", header.c_str());
	}
	std::string body = getBody(encryptionInfo.getKeyName(), encryptionInfo.getIv(), encryptionInfo.getKey());

	LOG(INFO, "http body : %s", body.c_str());

	// 4. call HttpClient to get response
	hc->init();
	hc->setURL(url);
	hc->setHeaders(headers);
	hc->setBody(body);
	std::string response = hc->post();
	hc->destroy();
	// 5. convert json to map
	ptree map = fromJson(response);
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
	//6. base64 decode key from map

	

	//7. return key
	return base64Decode(material);
}

std::string KmsService::getKmsUrl(const std::string &keyVersionName)
{
	return "http://localhost:16000/kms/v1/keyversion/" + hc->escape(keyVersionName)  + "/_eek?eek_op=decrypt&user.name=abai";	
}

const std::vector<std::string> KmsService::getKmsHeaders()
{
	std::vector<std::string> headers;
	headers.push_back("Content-Type: application/json");
 	headers.push_back("Accept: *");
 	return headers;
}

std::string KmsService::getBody(const std::string &name, const std::string &iv, const std::string &material)
{
	ptree map;
	map.put("name", name);
	map.put("iv", base64Encode(iv));
	map.put("material", base64Encode(material));
	return toJson(map); 
}

std::string	KmsService::base64Encode(const std::string &data)
{
	char * buffer;
	size_t len;
	gsasl_base64_to(data.c_str(), data.size(), &buffer, &len);
	std::string result;
	result.assign(buffer, len);
	free(buffer);
	return result;	
}

std::string	KmsService::base64Decode(const std::string &data)
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

