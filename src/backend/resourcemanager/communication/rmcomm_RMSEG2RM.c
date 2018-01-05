/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "envswitch.h"
#include "communication/rmcomm_RMSEG2RM.h"
#include "communication/rmcomm_Message.h"
#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_RMSEG_RM_Protocol.h"
#include "dynrm.h"
#include "utils/memutilities.h"
#include "utils/simplestring.h"
#include "utils/linkedlist.h"

#include <json-c/json.h>
#include "cetcd.h"

#define SEGSTAT_TMPDIRCOUNT "TmpDirCount"
#define SEGSTAT_TMPDIRBROKENCOUNT "TmpDirBrokenCount"
#define SEGSTAT_RESERVED "Reserved"
#define SEGSTAT_RMSTARTTIMESTAMP "RMStartTimestamp"
#define SEGSTAT_REQUESTHEAD "requesthead"
#define SEGSTAT_SIZE "size"
#define SEGSTAT_ADDRATTROFFSET "AddressAttributeOffset"
#define SEGSTAT_ADDRCONTENTOFFSET "AddressContentOffset"
#define SEGSTAT_HOSTNAMEOFFSET "HostNameOffset"
#define SEGSTAT_HOSTNAMELEN "HostNameLen"
#define SEGSTAT_PORT "port"
#define SEGSTAT_GRMHOSTNAMEOFFSET "GRMHostNameOffset"
#define SEGSTAT_GRMHOSTNAMELEN "GRMHostNameLen"
#define SEGSTAT_GRMRACKNAMEOFFSET "GRMRackNameOffset"
#define SEGSTAT_GRMRACKNAMELEN "GRMRackNameLen"
#define SEGSTAT_HOSTADDRCOUNT "HostAddrCount"
#define SEGSTAT_FAILEDTMPDIROFFSET "FailedTmpDirOffset"
#define SEGSTAT_FAILEDTMPDIRLEN "FailedTmpDirLen"
#define SEGSTAT_ID "ID"
#define SEGSTAT_FAILEDTMPDIR "FailedTmpDir"
#define SEGSTAT_MASTER "master"
#define SEGSTAT_STANDBY "standby"
#define SEGSTAT_ALIVE "alive"
#define SEGSTAT_ADDRARRAY "addressArray"
#define SEGSTAT_HOSTNAME "hostname"
#define SEGSTAT_GRMHOSTNAME "GRMhostname"
#define SEGSTAT_GRMRACKNAME "GRMRackname"
#define SEGSTAT_FAILEDTMPDIRNUM "FailedTmpDirNum"
#define SEGSTAT_FTSAVAILABLE "FTSAvailable"
#define SEGSTAT_GRMHANDLED "GRMHandled"
#define SEGSTAT_FTSTOTALMEM "FTSTotalMemoryMB"
#define SEGSTAT_FTSTOTALCORE "FTSTotalCore"
#define SEGSTAT_GRMTOTALMEM "GRMTotalMemoryMB"
#define SEGSTAT_GRMTOTALCORE "GRMTotalCore"
#define SEGSTAT_STATUSDESC "StatusDesc"
#define SEGSTAT_SEGINFO "SegInfo"
#define SEGSTAT_SEGSTATDATA "segStatData"

void receivedIMAliveResponse(AsyncCommMessageHandlerContext  context,
							 uint16_t						 messageid,
							 uint8_t						 mark1,
							 uint8_t						 mark2,
							 char 							*buffer,
							 uint32_t						 buffersize);
void sentIMAlive(AsyncCommMessageHandlerContext context);
void sentIMAliveError(AsyncCommMessageHandlerContext context);
void sentIMAliveCleanUp(AsyncCommMessageHandlerContext context);

/******************************************************************************
 * I aM Alive.
 *
 * Request:
 *         |<----------- 64 bits (8 bytes) ----------->|
 *         +----------+--------------------------------+
 *         |  TDC     |  BDC     |     Reserved        |
 * 		   +----------+----------+---------------------+
 *         |                                           |
 *         |             Machine ID info               |
 *         |                                           |
 * 		   +-------------------------------------------+     _____ 64bit aligned
 *
 * Response:
 *         |<----------- 64 bits (8 bytes) ----------->|
 *         +---------------------+---------------------+
 *         |  heartbeat result   |    Reserved   	   |
 * 		   +---------------------+---------------------+     _____ 64bit aligned
 *
 ******************************************************************************/


int sendIMAliveToEtcd() {

	/*Creating a json object*/
	json_object * etcdvalue = json_object_new_object();

	/*1.Add head */
	json_object * requesthead = json_object_new_object();
	json_object_object_add(requesthead, SEGSTAT_TMPDIRCOUNT, json_object_new_int(getDQueueLength(&DRMGlobalInstance->LocalHostTempDirectories)));
	json_object_object_add(requesthead, SEGSTAT_TMPDIRBROKENCOUNT, json_object_new_int(DRMGlobalInstance->LocalHostStat->FailedTmpDirNum));
	json_object_object_add(requesthead, SEGSTAT_RESERVED, json_object_new_int(0));
	json_object_object_add(requesthead, SEGSTAT_RMSTARTTIMESTAMP, json_object_new_int64(DRMGlobalInstance->ResourceManagerStartTime));
	json_object_object_add(etcdvalue, SEGSTAT_REQUESTHEAD, requesthead);

	/*2.Convert SegInfoData to a json object */
	json_object * seginfo = json_object_new_object();
	char *host = "";
	if (DRMGlobalInstance->LocalHostStat->Info.HostNameLen != 0)
		host = GET_SEGINFO_HOSTNAME(&DRMGlobalInstance->LocalHostStat->Info);

	char * GRMhost = "";
	if (DRMGlobalInstance->LocalHostStat->Info.GRMHostNameLen != 0)
		GRMhost = GET_SEGINFO_GRMHOSTNAME(&DRMGlobalInstance->LocalHostStat->Info);

	char * GRMRackname = "";
	if (DRMGlobalInstance->LocalHostStat->Info.GRMRackNameLen != 0)
		GRMRackname = GET_SEGINFO_GRMRACKNAME(&DRMGlobalInstance->LocalHostStat->Info);

	char * FailedTmpDir = "";
	if (DRMGlobalInstance->LocalHostStat->Info.FailedTmpDirLen != 0)
		FailedTmpDir = GET_SEGINFO_FAILEDTMPDIR(&DRMGlobalInstance->LocalHostStat->Info);

	/* Add ip address array to a json array. */
	json_object *addressArray = json_object_new_array();
	for (int i = 0; i < DRMGlobalInstance->LocalHostStat->Info.HostAddrCount; ++i) {
		uint16_t __MAYBE_UNUSED attr = GET_SEGINFO_ADDR_ATTR_AT(&DRMGlobalInstance->LocalHostStat->Info, i);
		Assert(IS_SEGINFO_ADDR_STR(attr));
		AddressString straddr = NULL;
		getSegInfoHostAddrStr(&DRMGlobalInstance->LocalHostStat->Info, i, &straddr);
		json_object_array_add(addressArray, json_object_new_string(straddr->Address));
	}

	json_object_object_add(seginfo, SEGSTAT_SIZE, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.Size));
	json_object_object_add(seginfo, SEGSTAT_ADDRATTROFFSET, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.AddressAttributeOffset));
	json_object_object_add(seginfo, SEGSTAT_ADDRCONTENTOFFSET, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.AddressContentOffset));
	json_object_object_add(seginfo, SEGSTAT_HOSTNAMEOFFSET, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.HostNameOffset));
	json_object_object_add(seginfo, SEGSTAT_HOSTNAMELEN, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.HostNameLen));
	json_object_object_add(seginfo, SEGSTAT_PORT, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.port));
	json_object_object_add(seginfo, SEGSTAT_GRMHOSTNAMEOFFSET, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.GRMHostNameOffset));
	json_object_object_add(seginfo, SEGSTAT_GRMHOSTNAMELEN, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.GRMHostNameLen));
	json_object_object_add(seginfo, SEGSTAT_GRMRACKNAMEOFFSET, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.GRMRackNameOffset));
	json_object_object_add(seginfo, SEGSTAT_GRMRACKNAMELEN, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.GRMRackNameLen));
	json_object_object_add(seginfo, SEGSTAT_HOSTADDRCOUNT, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.HostAddrCount));
	json_object_object_add(seginfo, SEGSTAT_FAILEDTMPDIROFFSET, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.FailedTmpDirOffset));
	json_object_object_add(seginfo, SEGSTAT_FAILEDTMPDIRLEN, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.FailedTmpDirLen));
	json_object_object_add(seginfo, SEGSTAT_ID, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.ID));
	json_object_object_add(seginfo, SEGSTAT_FAILEDTMPDIR, json_object_new_string(FailedTmpDir));
	json_object_object_add(seginfo, SEGSTAT_MASTER, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.master));
	json_object_object_add(seginfo, SEGSTAT_STANDBY, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.standby));
	json_object_object_add(seginfo, SEGSTAT_ALIVE, json_object_new_int(DRMGlobalInstance->LocalHostStat->Info.alive));
	json_object_object_add(seginfo, SEGSTAT_ADDRARRAY, addressArray);
	json_object_object_add(seginfo, SEGSTAT_HOSTNAME, json_object_new_string(host));
	json_object_object_add(seginfo, SEGSTAT_GRMHOSTNAME, json_object_new_string(GRMhost));
	json_object_object_add(seginfo, SEGSTAT_GRMRACKNAME, json_object_new_string(GRMRackname));

	/*3.Convert SegStatData to a json object */
	json_object * segstat = json_object_new_object();

	json_object_object_add(segstat, SEGSTAT_FAILEDTMPDIRNUM, json_object_new_int(DRMGlobalInstance->LocalHostStat->FailedTmpDirNum));
	json_object_object_add(segstat, SEGSTAT_FTSAVAILABLE, json_object_new_boolean(DRMGlobalInstance->LocalHostStat->FTSAvailable));
	json_object_object_add(segstat, SEGSTAT_GRMHANDLED, json_object_new_boolean(DRMGlobalInstance->LocalHostStat->GRMHandled));
	json_object_object_add(segstat, SEGSTAT_FTSTOTALMEM, json_object_new_int(DRMGlobalInstance->LocalHostStat->FTSTotalMemoryMB));
	json_object_object_add(segstat, SEGSTAT_FTSTOTALCORE, json_object_new_int(DRMGlobalInstance->LocalHostStat->FTSTotalCore));
	json_object_object_add(segstat, SEGSTAT_GRMTOTALMEM, json_object_new_int(DRMGlobalInstance->LocalHostStat->GRMTotalMemoryMB));
	json_object_object_add(segstat, SEGSTAT_GRMTOTALCORE, json_object_new_int(DRMGlobalInstance->LocalHostStat->GRMTotalCore));
	json_object_object_add(segstat, SEGSTAT_STATUSDESC, json_object_new_int(DRMGlobalInstance->LocalHostStat->StatusDesc));
	json_object_object_add(segstat, SEGSTAT_RMSTARTTIMESTAMP, json_object_new_int64(DRMGlobalInstance->LocalHostStat->RMStartTimestamp));
	json_object_object_add(segstat, SEGSTAT_SEGINFO, seginfo);

	json_object_object_add(etcdvalue, SEGSTAT_SEGSTATDATA, segstat);

	/* If the Etcd string is same as the previous segment info, just relet the ttl of etcd.
	 * Otherwise reset ttl value.
	 */
	bool isSameEtcdString = false;
	if (DRMGlobalInstance->etcdInfo.initialized == false) {
		DRMGlobalInstance->etcdInfo.etcd_string = NULL;
		DRMGlobalInstance->etcdInfo.etcd_string_old = NULL;
		DRMGlobalInstance->etcdInfo.initialized = true;
		DRMGlobalInstance->etcdInfo.etcd_string = json_object_to_json_string(etcdvalue);
		DRMGlobalInstance->etcdInfo.etcd_string_old = json_object_to_json_string(etcdvalue);

		char * addrIP = rm_etcd_server_ip;
		cetcd_array_init(&DRMGlobalInstance->etcdInfo.addrs, 1);
		cetcd_array_append(&DRMGlobalInstance->etcdInfo.addrs, addrIP);
		cetcd_client_init(&DRMGlobalInstance->etcdInfo.cli, &DRMGlobalInstance->etcdInfo.addrs);
		elog(LOG, "initialize etcd client and server");
	} else {
		DRMGlobalInstance->etcdInfo.etcd_string = json_object_to_json_string(etcdvalue);
		if (strcmp(DRMGlobalInstance->etcdInfo.etcd_string,DRMGlobalInstance->etcdInfo.etcd_string_old) == 0)
			isSameEtcdString = true;
		DRMGlobalInstance->etcdInfo.etcd_string_old = DRMGlobalInstance->etcdInfo.etcd_string;
		elog(LOG, "isSameEtcdString is %d", isSameEtcdString);
	}

	elog(LOG, "The json object created: %s",DRMGlobalInstance->etcdInfo.etcd_string);
	elog(LOG, "The old json object created: %s",DRMGlobalInstance->etcdInfo.etcd_string_old);

	cetcd_response *resp;
	char key[MAXPGPATH] = {0};
	sprintf(key, "%s%s", rm_etcd_server_dir, host);
	elog(LOG, "key of etcd is %s, serverDir is %s", key, rm_etcd_server_dir);
	if (isSameEtcdString) {
		resp = cetcd_update(&DRMGlobalInstance->etcdInfo.cli, key, 0,
				rm_segment_etcd_ttl, 1);
		if (resp == NULL) {
			elog(WARNING, "resp is NULL when cetcd_update");
			return FUNC_RETURN_OK;
		}
		if (resp->err) {
			elog(LOG, "cetcd_update error :%d, %s (%s)\n", resp->err->ecode, resp->err->message, resp->err->cause);
		}
	} else {
		resp = cetcd_set(&DRMGlobalInstance->etcdInfo.cli, key,
				DRMGlobalInstance->etcdInfo.etcd_string, rm_segment_etcd_ttl);
		if (resp == NULL) {
			elog(WARNING, "resp is NULL when cetcd_set");
			return FUNC_RETURN_OK;
		}
		if (resp->err) {
			elog(LOG, "cetcd_set error :%d, %s (%s)\n", resp->err->ecode, resp->err->message, resp->err->cause);
		}
	}
	if (resp)
		cetcd_response_release(resp);

	return FUNC_RETURN_OK;
}


int sendIMAlive(int  *errorcode,
				char *errorbuf,
				int	  errorbufsize)
{
	int 				res 					= FUNC_RETURN_OK;
	AsyncCommBuffer		newcommbuffer			= NULL;

	Assert( DRMGlobalInstance->LocalHostStat != NULL );

	/* Build request. */
	SelfMaintainBufferData tosend;
	initializeSelfMaintainBuffer(&tosend, PCONTEXT);

	RPCRequestHeadIMAliveData requesthead;
	requesthead.TmpDirCount 	  = getDQueueLength(&DRMGlobalInstance->LocalHostTempDirectories);
	requesthead.TmpDirBrokenCount = DRMGlobalInstance->LocalHostStat->FailedTmpDirNum;
	requesthead.Reserved		  = 0;
	requesthead.RMStartTimestamp  = DRMGlobalInstance->ResourceManagerStartTime;

	appendSMBVar(&tosend, requesthead);
	appendSelfMaintainBuffer(&tosend,
							 (char *)(DRMGlobalInstance->LocalHostStat),
							 offsetof(SegStatData, Info) +
							 DRMGlobalInstance->LocalHostStat->Info.Size);

	/* Set content to send and add to AsyncComm framework. */
	AsyncCommMessageHandlerContext context =
			rm_palloc0(AsyncCommContext,
					   sizeof(AsyncCommMessageHandlerContextData));
	context->inMessage				 = false;
	context->UserData  				 = NULL;
	context->MessageRecvReadyHandler = NULL;
	context->MessageRecvedHandler 	 = receivedIMAliveResponse;
	context->MessageSendReadyHandler = NULL;
	context->MessageSentHandler		 = sentIMAlive;
	context->MessageErrorHandler 	 = sentIMAliveError;
	context->MessageCleanUpHandler	 = sentIMAliveCleanUp;

	/* Connect to HAWQ RM server */

	res = registerAsyncConnectionFileDesc(DRMGlobalInstance->SendToStandby?
										  standby_addr_host:
										  master_addr_host,
										  rm_master_port,
										  ASYNCCOMM_READBYTES | ASYNCCOMM_WRITEBYTES,
										  &AsyncCommBufferHandlersMessage,
										  context,
										  &newcommbuffer);
	if ( res != FUNC_RETURN_OK )
	{
		rm_pfree(AsyncCommContext, context);
		elog(LOG, "failed to register asynchronous connection for sending "
			      "IMAlive message. %d", res);
		/* Always switch if fail to register connection here. */
		switchIMAliveSendingTarget();
		return res;
	}

	buildMessageToCommBuffer(newcommbuffer,
							 tosend.Buffer,
							 tosend.Cursor + 1,
							 REQUEST_RM_IMALIVE,
							 0,
							 0);

	destroySelfMaintainBuffer(&tosend);

	context->AsyncBuffer = newcommbuffer;

	InitHandler_Message(newcommbuffer);

	return FUNC_RETURN_OK;
}


void receivedIMAliveResponse(AsyncCommMessageHandlerContext  context,
							 uint16_t						 messageid,
							 uint8_t						 mark1,
							 uint8_t						 mark2,
							 char 							*buffer,
							 uint32_t						 buffersize)
{
	RPCResponseIMAlive response = (RPCResponseIMAlive)buffer;
	if ( messageid != RESPONSE_RM_IMALIVE ||
		 buffersize != sizeof(RPCResponseIMAliveData) ) {
		elog(WARNING, "Segment's resource manager received wrong response for "
					  "heart-beat request.");
		switchIMAliveSendingTarget();
	}
	else
	{
		/* Should always be a FUNC_RETURN_OK result. */
		Assert(response->Result == FUNC_RETURN_OK);
		elog(DEBUG5, "Segment's resource manager gets response of heart-beat "
					 "request successfully.");
	}
	closeFileDesc(context->AsyncBuffer);
}

void sentIMAlive(AsyncCommMessageHandlerContext context)
{
	/* Do nothing. */
}

void sentIMAliveError(AsyncCommMessageHandlerContext context)
{
	if(DRMGlobalInstance->SendToStandby)
		elog(WARNING, "Segment's resource manager sending IMAlive message "
					  "switches from standby to master");
	else
		elog(WARNING, "Segment's resource manager sending IMAlive message "
					  "switches from master to standby");
	switchIMAliveSendingTarget();
}

void sentIMAliveCleanUp(AsyncCommMessageHandlerContext context)
{
	/* Do nothing. */
}
