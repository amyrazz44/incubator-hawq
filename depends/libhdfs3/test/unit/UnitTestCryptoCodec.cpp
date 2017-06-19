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
#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "client/FileSystem.h"
#include "client/FileSystemImpl.h"
#include "client/FileSystemInter.h"
#include "client/OutputStream.h"
#include "client/OutputStreamImpl.h"
#include "client/Packet.h"
#include "client/Pipeline.h"
#include "DateTime.h"
#include "MockFileSystemInter.h"
#include "MockCryptoCodec.h"
#include "MockHttpClient.h"
#include "MockLeaseRenewer.h"
#include "MockPipeline.h"
#include "NamenodeStub.h"
#include "server/ExtendedBlock.h"
#include "TestDatanodeStub.h"
#include "TestUtil.h"
#include "Thread.h"
#include "XmlConfig.h"
#include "client/KmsClientProvider.h"

#include <string>

using namespace Hdfs;
using namespace Internal;
using namespace Hdfs::Mock;
using namespace testing;
using ::testing::AtLeast;


class TestCryptoCodec: public ::testing::Test {
public:
    TestCryptoCodec() {
        
    }

    ~TestCryptoCodec() {
    }

protected:
};


TEST_F(TestCryptoCodec, KmsGetKey_Success) {
	FileEncryptionInfo encryptionInfo;
	encryptionInfo.setKeyName("KmsName");
    encryptionInfo.setIv("KmsIv");
    encryptionInfo.setEzKeyVersionName("KmsVersionName");
	encryptionInfo.setKey("KmsKey");
	Config conf;
	shared_ptr<SessionConfig> sessionConf = new SessionConfig(conf);
	shared_ptr<RpcAuth> auth = new RpcAuth();
	KmsClientProvider ks(auth, conf);
	MockHttpClient hc;
	//todo
	std::string url = ks.getKmsUrl("KmsVersionName");
	std::vector<std::string> headers = ks.getKmsHeaders();
	std::string body = ks.getBody(encryptionInfo.getKeyName(), encryptionInfo.getIv(), encryptionInfo.getKey());
	EXPECT_CALL(hc, HttpPost()).Times(1).WillOnce(Return(hc.getPostResult()));	
	std::string KmsKey = ks.getKey(encryptionInfo);

	ASSERT_STREQ("testmaterial", KmsKey.c_str());
}


TEST_F(TestCryptoCodec, encode_Success) {
	FileEncryptionInfo encryptionInfo;
	encryptionInfo.setKeyName("ESKeyName");
	encryptionInfo.setIv("ESIv");
	encryptionInfo.setEzKeyVersionName("ESVersionName");


	char buf[1024] = "encode hello world";
	std::string Key[3] = {
		"012345678901234567890123456789ab",
		"0123456789012345",
		"OTHER LENGTH"
	};
	for(int i=0; i<3; i++) {
		encryptionInfo.setKey(Key[i]);
		MockHttpClient hc;
		KmsClientProvider ks(&hc);
		CryptoCodec es(&encryptionInfo, &ks);
		EXPECT_CALL(hc, HttpPost()).Times(2).WillRepeatedly(Return(hc.getPostResult()));
		std::string encodeStr = es.encode(buf, strlen(buf));
		ASSERT_NE(0, memcmp(buf, encodeStr.c_str(), strlen(buf)));	

		std::string decodeStr = es.decode(encodeStr.c_str(), strlen(buf));
		ASSERT_STREQ(decodeStr.c_str(), buf); 
	}
}
