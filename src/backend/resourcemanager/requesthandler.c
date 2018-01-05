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

#include "dynrm.h"
#include "envswitch.h"
#include "utils/linkedlist.h"
#include "utils/memutilities.h"
#include "utils/network_utils.h"
#include "utils/kvproperties.h"
#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_QD2RM.h"
#include "communication/rmcomm_QD_RM_Protocol.h"
#include "communication/rmcomm_RMSEG_RM_Protocol.h"
#include "communication/rmcomm_RM2RMSEG.h"
#include "communication/rmcomm_RMSEG2RM.h"
#include "resourceenforcer/resourceenforcer.h"

#include "resourcemanager.h"
#include <json-c/json.h>

/*
 * The MAIN ENTRY of request handler.
 * The implementation of all request handlers are :
 * 		requesthandler.c 		: handlers for QD-RM resource negotiation RPC
 * 		requesthandler_ddl.c	: handlers for QD-RM DDL manipulations
 * 		requesthandler_RMSEG.c	: handlers for QE-RMSEG, RM-RMSEG RPC
 */
#define RMSEG_INBUILDHOST SMBUFF_HEAD(SegStat, &localsegstat)

#define RETRIEVE_CONNTRACK(res, connid, conntrack)                      	   \
	if ( (*conntrack)->ConnID == INVALID_CONNID ) {                            \
		res = retrieveConnectionTrack((*conntrack), connid);				   \
		if ( res != FUNC_RETURN_OK ) {                                         \
			elog(LOG, "invalid resource context with id %d.", connid);  	   \
			goto sendresponse;                                                 \
		}                                                                      \
		elog(DEBUG5, "HAWQ RM :: Fetched existing connection track "           \
					 "ID=%d, Progress=%d.",                                    \
					 (*conntrack)->ConnID,                                     \
					 (*conntrack)->Progress);                                  \
	}

#define JSON_PARSE_INT(body, param, item, result)                              \
		json_object_object_get_ex((body), (param), &(item));                   \
		if (NULL == (item))                                                    \
			goto failed;                                                       \
		(result) = json_object_get_int(item);                                  \
		elog(LOG, "%s is %d", param, result);                                  \

#define JSON_PARSE_STR(body, param, item, result)                              \
		json_object_object_get_ex((body), (param), &(item));                   \
		if (NULL == (item))                                                    \
			goto failed;                                                       \
		(result) = json_object_get_string(item);                               \
		elog(LOG, "%s is %s", param, result);                                  \

#define JSON_PARSE_BOOL(body, param, item, result)                             \
		json_object_object_get_ex((body), (param), &(item));                   \
		if (NULL == (item))                                                    \
			goto failed;                                                       \
		(result) = json_object_get_boolean(item);                              \
		elog(LOG, "%s is %s", param, result);                                  \
/*
 * Handle the request of REGISTER CONNECTION.
 */
bool handleRMRequestConnectionReg(void **arg)
{
	static char 			errorbuf[ERRORMESSAGE_SIZE];
	int						res			= FUNC_RETURN_OK;
	ConnectionTrack 		conntrack	= (ConnectionTrack )(*arg);
	SelfMaintainBufferData 	responsedata;
	RPCResponseRegisterConnectionInRMByStrData response;

	initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

	/* Parse request. */
	strncpy(conntrack->UserID,
			SMBUFF_CONTENT(&(conntrack->MessageBuff)),
			sizeof(conntrack->UserID)-1);
	conntrack->UserID[sizeof(conntrack->UserID)-1] = '\0';

	/* Handle the request. */
	res = registerConnectionByUserID(conntrack, errorbuf, sizeof(errorbuf));
	if ( res == FUNC_RETURN_OK )
	{
		/* Allocate connection id and track this connection. */
		res = useConnectionID(&(conntrack->ConnID));
		if ( res == FUNC_RETURN_OK )
		{
			trackConnectionTrack(conntrack);
			elog(LOG, "ConnID %d. Resource manager tracked connection.",
					  conntrack->ConnID);
			response.Result = FUNC_RETURN_OK;
			response.ConnID = conntrack->ConnID;
		}
		else
		{
			Assert( res == CONNTRACK_CONNID_FULL );
			snprintf(errorbuf, sizeof(errorbuf),
					 "cannot accept more resource context instance");
			elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
			/* No connection id resource. Return occupation in resource queue. */
			returnConnectionToQueue(conntrack, false);
			response.Result = res;
			response.ConnID = INVALID_CONNID;
		}
	}
	else
	{
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		response.Result = res;
		response.ConnID = INVALID_CONNID;
	}

	/* Build response. */
	appendSMBVar(&responsedata, response);
	if ( response.Result != FUNC_RETURN_OK )
	{
		appendSMBStr(&responsedata, errorbuf);
		appendSelfMaintainBufferTill64bitAligned(&responsedata);
	}

	buildResponseIntoConnTrack(conntrack,
				   	   	   	   SMBUFF_CONTENT(&responsedata),
							   getSMBContentSize(&responsedata),
							   conntrack->MessageMark1,
							   conntrack->MessageMark2,
							   RESPONSE_QD_CONNECTION_REG);

	destroySelfMaintainBuffer(&responsedata);
	elog(DEBUG3, "ConnID %d. One connection register result %d.",
				 conntrack->ConnID,
				 response.Result);

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

/**
 * Handle the request of REGISTER CONNECTION.
 */
bool handleRMRequestConnectionRegByOID(void **arg)
{
	static char 		   errorbuf[ERRORMESSAGE_SIZE];
	int		   			   res			= FUNC_RETURN_OK;
	ConnectionTrack 	   conntrack	= (ConnectionTrack )(*arg);
	bool				   exist		= false;
	SelfMaintainBufferData responsedata;
	RPCResponseRegisterConnectionInRMByOIDData response;

	initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

	RPCRequestRegisterConnectionInRMByOID request =
		SMBUFF_HEAD(RPCRequestRegisterConnectionInRMByOID,
					&(conntrack->MessageBuff));

	/* Get user name from oid. */
	UserInfo reguser = getUserByUserOID(request->UseridOid, &exist);
	if ( !exist )
	{
		res = RESQUEMGR_NO_USERID;
		snprintf(errorbuf, sizeof(errorbuf),
				 "user oid " INT64_FORMAT "does not exist",
				 request->UseridOid);
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		goto exit;
	}
	else
	{
		/* Set user id string into connection track. */
		strncpy(conntrack->UserID, reguser->Name, sizeof(conntrack->UserID)-1);
	}

	/* Handle the request. */
	res = registerConnectionByUserID(conntrack, errorbuf, sizeof(errorbuf));

	if ( res == FUNC_RETURN_OK )
	{
		/* Allocate connection id and track this connection. */
		res = useConnectionID(&(conntrack->ConnID));
		if ( res == FUNC_RETURN_OK )
		{
			trackConnectionTrack(conntrack);
			elog(RMLOG, "ConnID %d. Resource manager tracked connection.",
					  	conntrack->ConnID);
			response.Result = FUNC_RETURN_OK;
			response.ConnID = conntrack->ConnID;
		}
		else {
			Assert( res == CONNTRACK_CONNID_FULL );
			snprintf(errorbuf, sizeof(errorbuf),
					 "cannot accept more resource context instance");
			elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
			/* No connection id resource. Return occupation in resource queue. */
			returnConnectionToQueue(conntrack, false);
			response.Result = res;
			response.ConnID = INVALID_CONNID;
		}
	}
	else {
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		response.Result = res;
		response.ConnID = INVALID_CONNID;
	}

exit:
	/* Build message saved in the connection track instance. */
	appendSMBVar(&responsedata, response);
	if ( response.Result != FUNC_RETURN_OK )
	{
		appendSMBStr(&responsedata, errorbuf);
		appendSelfMaintainBufferTill64bitAligned(&responsedata);
	}

	buildResponseIntoConnTrack(conntrack,
				   	   	   	   SMBUFF_CONTENT(&responsedata),
							   getSMBContentSize(&responsedata),
							   conntrack->MessageMark1,
							   conntrack->MessageMark2,
							   RESPONSE_QD_CONNECTION_REG_OID);
	destroySelfMaintainBuffer(&responsedata);
	elog(DEBUG3, "ConnID %d. One connection register result %d (OID).",
				 conntrack->ConnID,
				 response.Result);

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

/*
 * Handle UNREGISTER request.
 */
bool handleRMRequestConnectionUnReg(void **arg)
{
	static char 	 errorbuf[ERRORMESSAGE_SIZE];
	int      		 res	 	= FUNC_RETURN_OK;
	ConnectionTrack *conntrack 	= (ConnectionTrack *)arg;

	RPCRequestHeadUnregisterConnectionInRM request =
		SMBUFF_HEAD(RPCRequestHeadUnregisterConnectionInRM,
					&((*conntrack)->MessageBuff));

	elog(DEBUG3, "ConnID %d. Try to unregister.", request->ConnID);

	res = retrieveConnectionTrack((*conntrack), request->ConnID);
	if ( res != FUNC_RETURN_OK )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "the resource context is invalid or timed out");
		elog(WARNING, "ConnID %d. %s", request->ConnID, errorbuf);
		goto sendresponse;
	}
	elog(DEBUG3, "ConnID %d. Fetched existing connection track, progress=%d.",
				 (*conntrack)->ConnID,
				 (*conntrack)->Progress);

	/* Get connection ID. */
	request = SMBUFF_HEAD(RPCRequestHeadUnregisterConnectionInRM,
			  	  	  	  &((*conntrack)->MessageBuff));

	/*
	 * If this connection is waiting for resource allocated, cancel the request
	 * from resource queue.
	 */
	if ( (*conntrack)->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT )
	{
		cancelResourceAllocRequest((*conntrack), errorbuf, false);
		transformConnectionTrackProgress((*conntrack), CONN_PP_REGISTER_DONE);
	}
	/* If this connection has resource allocated, return the resource. */
	else if ( (*conntrack)->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_DONE )
	{
		returnResourceToResQueMgr((*conntrack));
	}
	/*
	 * If this connection has acquire resource not processed yet, we should
	 * remove that now. In this case, this connection should have registered.
	 */
	else if ( (*conntrack)->Progress == CONN_PP_REGISTER_DONE )
	{
		elog(WARNING, "resource manager finds possible not handled resource "
					  "request from ConnID %d.",
					  request->ConnID);
		removeResourceRequestInConnHavingReqeusts(request->ConnID);
	}
	else if ( !canTransformConnectionTrackProgress((*conntrack), CONN_PP_ESTABLISHED) )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "wrong resource context status for unregistering, %d",
				 (*conntrack)->Progress);

		elog(WARNING, "ConnID %d. %s", request->ConnID, errorbuf);
		res = REQUESTHANDLER_WRONG_CONNSTAT;
		goto sendresponse;
	}

	returnConnectionToQueue(*conntrack, false);

	elog(LOG, "ConnID %d. Connection is unregistered.", (*conntrack)->ConnID);

sendresponse:
	{
		SelfMaintainBufferData responsedata;
		initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

		RPCResponseUnregisterConnectionInRMData response;
		response.Result   = res;
		response.Reserved = 0;
		appendSMBVar(&responsedata, response);

		if ( response.Result != FUNC_RETURN_OK )
		{
			appendSMBStr(&responsedata, errorbuf);
			appendSelfMaintainBufferTill64bitAligned(&responsedata);
		}

		/* Build message saved in the connection track instance. */
		buildResponseIntoConnTrack((*conntrack),
							 	   SMBUFF_CONTENT(&responsedata),
								   getSMBContentSize(&responsedata),
								   (*conntrack)->MessageMark1,
								   (*conntrack)->MessageMark2,
								   RESPONSE_QD_CONNECTION_UNREG);
		destroySelfMaintainBuffer(&responsedata);

		if ( res == CONNTRACK_NO_CONNID )
		{
			transformConnectionTrackProgress((*conntrack),
											 CONN_PP_TRANSFORM_ERROR);
		}

		(*conntrack)->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, *conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	return true;
}

/**
 * Handle the request of ACQUIRE QUERY RESOURCE
 **/
bool handleRMRequestAcquireResource(void **arg)
{
	static char		 errorbuf[ERRORMESSAGE_SIZE];
	int				 res		= FUNC_RETURN_OK;
	ConnectionTrack *conntrack	= (ConnectionTrack *)arg;
	uint64_t		 reqtime	= gettime_microsec();

	/* If we run in YARN mode, we expect that we should try to get at least one
	 * available segment, and this requires at least once global resource manager
	 * cluster report returned.
	 */
	if ( reqtime - DRMGlobalInstance->ResourceManagerStartTime <=
		 rm_nocluster_timeout * 1000000LL &&
		 PRESPOOL->RBClusterReportCounter == 0 )
	{
		elog(DEBUG3, "resource manager defers the resource request.");
		return false;
	}

	/*
	 * If resource queue has no concrete capacity set yet, no need to handle
	 * the request.
	 */
	if ( PQUEMGR->RootTrack->QueueInfo->ClusterMemoryMB <= 0 )
	{
		elog(DEBUG3, "resource manager defers the resource request because the "
					 "resource queues have no valid resource capacities yet.");
		return false;
	}

	RPCRequestHeadAcquireResourceFromRM request =
		SMBUFF_HEAD(RPCRequestHeadAcquireResourceFromRM,
					&((*conntrack)->MessageBuff));

	elog(DEBUG3, "ConnID %d. Acquires query resource. Session id "INT64_FORMAT,
				 request->ConnID,
				 request->SessionID);

	res = retrieveConnectionTrack((*conntrack), request->ConnID);
	if ( res != FUNC_RETURN_OK )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "the resource context may be timed out");
		elog(WARNING, "ConnID %d. %s", request->ConnID, errorbuf);
		goto sendresponse;
	}
	elog(DEBUG3, "ConnID %d. Fetched existing connection track, progress=%d.",
				 (*conntrack)->ConnID,
				 (*conntrack)->Progress);

	request = SMBUFF_HEAD(RPCRequestHeadAcquireResourceFromRM,
						  &((*conntrack)->MessageBuff));
	if ( (*conntrack)->Progress != CONN_PP_REGISTER_DONE )
	{
		res = REQUESTHANDLER_WRONG_CONNSTAT;
		snprintf(errorbuf, sizeof(errorbuf),
				 "the resource context status is invalid");
		elog(WARNING, "ConnID %d. %s", (*conntrack)->ConnID, errorbuf);
		goto sendresponse;
	}

	/*--------------------------------------------------------------------------
	 * We firstly check if the cluster has too many unavailable segments, which
	 * is measured by rm_rejectrequest_nseg_limit. The expected cluster size is
	 * loaded from counting hosts in $GPHOME/etc/slaves. Resource manager rejects
	 * query  resource requests at once if currently there are too many segments
	 * unavailable.
	 *--------------------------------------------------------------------------
	 */
	Assert(PRESPOOL->SlavesHostCount > 0);
	int rejectlimit = ceil(PRESPOOL->SlavesHostCount * rm_rejectrequest_nseg_limit);
	int unavailcount = PRESPOOL->SlavesHostCount - PRESPOOL->AvailNodeCount;
	if ( unavailcount > rejectlimit )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "%d of %d segments %s unavailable, exceeds %.1f%% defined in GUC hawq_rm_rejectrequest_nseg_limit. "
				 "The allocation request is rejected.",
				 unavailcount,
				 PRESPOOL->SlavesHostCount,
				 unavailcount == 1 ? "is" : "are",
				 rm_rejectrequest_nseg_limit*100.0);
		elog(WARNING, "ConnID %d. %s", (*conntrack)->ConnID, errorbuf);
		res = RESOURCEPOOL_TOO_MANY_UAVAILABLE_HOST;
		goto sendresponse;
	}

	/* Get scan size. */
	request = SMBUFF_HEAD(RPCRequestHeadAcquireResourceFromRM,
			  	  	  	  &((*conntrack)->MessageBuff));

	(*conntrack)->SliceSize				= request->SliceSize;
	(*conntrack)->IOBytes				= request->IOBytes;
	(*conntrack)->SegPreferredHostCount = request->NodeCount;
	(*conntrack)->MaxSegCountFixed      = request->MaxSegCountFix;
	(*conntrack)->MinSegCountFixed      = request->MinSegCountFix;
	(*conntrack)->SessionID				= request->SessionID;
	(*conntrack)->VSegLimitPerSeg		= request->VSegLimitPerSeg;
	(*conntrack)->VSegLimit				= request->VSegLimit;
	(*conntrack)->StatVSegMemoryMB		= request->StatVSegMemoryMB;
	(*conntrack)->StatNVSeg				= request->StatNVSeg;

	/* Get preferred nodes. */
	buildSegPreferredHostInfo((*conntrack));

	elog(RMLOG, "ConnID %d. Session ID " INT64_FORMAT " "
				"Scanning "INT64_FORMAT" io bytes "
				"by %d slices with %d preferred segments. "
				"Expect %d vseg (MIN %d). "
				"Each segment has maximum %d vseg. "
				"Query has maximum %d vseg. "
				"Statement quota %d MB x %d vseg",
				(*conntrack)->ConnID,
				(*conntrack)->SessionID,
				(*conntrack)->IOBytes,
				(*conntrack)->SliceSize,
				(*conntrack)->SegPreferredHostCount,
				(*conntrack)->MaxSegCountFixed,
				(*conntrack)->MinSegCountFixed,
				(*conntrack)->VSegLimitPerSeg,
				(*conntrack)->VSegLimit,
				(*conntrack)->StatVSegMemoryMB,
				(*conntrack)->StatNVSeg);

	if ( (*conntrack)->StatNVSeg > 0 )
	{
		elog(LOG, "ConnID %d. Statement level resource quota is active. "
				  "Expect resource ( %d MB ) x %d.",
				  (*conntrack)->ConnID,
				  (*conntrack)->StatVSegMemoryMB,
				  (*conntrack)->StatNVSeg);
	}

	res = acquireResourceFromResQueMgr((*conntrack), errorbuf, sizeof(errorbuf));
	if ( res != FUNC_RETURN_OK )
	{
		goto sendresponse;
	}
	(*conntrack)->ResRequestTime = reqtime;
	(*conntrack)->LastActTime    = (*conntrack)->ResRequestTime;

	return true;

sendresponse:
	{
		/* Send error message. */
		RPCResponseAcquireResourceFromRMERRORData errresponse;
		errresponse.Result   = res;
		errresponse.Reserved = 0;

		SelfMaintainBufferData responsedata;
		initializeSelfMaintainBuffer(&responsedata, PCONTEXT);
		appendSMBVar(&responsedata, errresponse);
		appendSMBStr(&responsedata, errorbuf);
		appendSelfMaintainBufferTill64bitAligned(&responsedata);

		buildResponseIntoConnTrack((*conntrack),
								   SMBUFF_CONTENT(&responsedata),
								   getSMBContentSize(&responsedata),
								   (*conntrack)->MessageMark1,
								   (*conntrack)->MessageMark2,
								   RESPONSE_QD_ACQUIRE_RESOURCE);
		destroySelfMaintainBuffer(&responsedata);

		transformConnectionTrackProgress((*conntrack), CONN_PP_TRANSFORM_ERROR);

		(*conntrack)->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, *conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
	}
	return true;
}


/**
 * Handle RETURN RESOURCE request.
 *
 */
bool handleRMRequestReturnResource(void **arg)
{
	static char		errorbuf[ERRORMESSAGE_SIZE];
	int      		res		    = FUNC_RETURN_OK;
	ConnectionTrack *conntrack  = (ConnectionTrack *)arg;

	RPCRequestHeadReturnResource request =
		SMBUFF_HEAD(RPCRequestHeadReturnResource, &((*conntrack)->MessageBuff));

	elog(DEBUG3, "ConnID %d. Returns query resource.", request->ConnID);

	res = retrieveConnectionTrack((*conntrack), request->ConnID);
	if ( res != FUNC_RETURN_OK )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "the resource context may be timed out");
		elog(WARNING, "ConnID %d. %s", request->ConnID, errorbuf);
		goto sendresponse;
	}
	elog(DEBUG3, "ConnID %d. Fetched existing connection track, progress=%d.",
				 (*conntrack)->ConnID,
				 (*conntrack)->Progress);

	/* Get connection ID. */
	request = SMBUFF_HEAD(RPCRequestHeadReturnResource,
						  &((*conntrack)->MessageBuff));

	if ( (*conntrack)->Progress != CONN_PP_RESOURCE_QUEUE_ALLOC_DONE )
	{
		res = REQUESTHANDLER_WRONG_CONNSTAT;
		snprintf(errorbuf, sizeof(errorbuf),
				 "the resource context status is invalid");
		elog(WARNING, "ConnID %d. %s", (*conntrack)->ConnID, errorbuf);
		goto sendresponse;
	}

	/* Return the resource. */
	returnResourceToResQueMgr(*conntrack);

	elog(LOG, "ConnID %d. Returned resource.", (*conntrack)->ConnID);

sendresponse:
	{
		SelfMaintainBufferData responsedata;
		initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

		RPCResponseHeadReturnResourceData response;
		response.Result   = res;
		response.Reserved = 0;
		appendSMBVar(&responsedata, response);

		if ( response.Result != FUNC_RETURN_OK )
		{
			appendSMBStr(&responsedata, errorbuf);
			appendSelfMaintainBufferTill64bitAligned(&responsedata);
		}

		buildResponseIntoConnTrack((*conntrack),
	                               SMBUFF_CONTENT(&responsedata),
								   getSMBContentSize(&responsedata),
								   (*conntrack)->MessageMark1,
								   (*conntrack)->MessageMark2,
								   RESPONSE_QD_RETURN_RESOURCE );
		destroySelfMaintainBuffer(&responsedata);
		if ( res == CONNTRACK_NO_CONNID )
		{
			transformConnectionTrackProgress((*conntrack), CONN_PP_TRANSFORM_ERROR);
		}

		(*conntrack)->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, *conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
	}
	return true;
}

/*
 * Parse json object from etcd server which is segment send seginfo to.
 */
SegStat handleEtcdJsonParse(char *key, char *value, char *fts_client_ip, int fts_client_ip_len)
{
	SelfMaintainBufferData localsegstat;
	initializeSelfMaintainBuffer(&localsegstat, PCONTEXT);
	prepareSelfMaintainBuffer(&localsegstat, sizeof(SegStatData), true);
	json_object *jobj, *header, *body, *info, *item;
	jobj = json_tokener_parse(value);
	/* Parse each object from json string. */
	if (is_error(jobj))
		goto failed;
	header = json_object_object_get(jobj, "requesthead");
	if (NULL == header)
		goto failed;
	body = json_object_object_get(jobj, "segStatData");
	if (NULL == body)
		goto failed;
	info = json_object_object_get(body, "SegInfo");
	if (NULL == info)
		goto failed;
	/* Parse each item from json string. */
	JSON_PARSE_INT(header, SEGSTAT_TMPDIRBROKENCOUNT, item, RMSEG_INBUILDHOST->FailedTmpDirNum)
	JSON_PARSE_INT(body, SEGSTAT_FTSAVAILABLE, item, RMSEG_INBUILDHOST->FTSAvailable)
	JSON_PARSE_BOOL(body, SEGSTAT_GRMHANDLED, item, RMSEG_INBUILDHOST->GRMHandled)
	JSON_PARSE_INT(body, SEGSTAT_FTSTOTALMEM, item, RMSEG_INBUILDHOST->FTSTotalMemoryMB)
	JSON_PARSE_INT(body, SEGSTAT_FTSTOTALCORE, item, RMSEG_INBUILDHOST->FTSTotalCore)
	JSON_PARSE_INT(body, SEGSTAT_GRMTOTALMEM, item, RMSEG_INBUILDHOST->GRMTotalMemoryMB)
	JSON_PARSE_INT(body, SEGSTAT_GRMTOTALCORE, item, RMSEG_INBUILDHOST->GRMTotalCore)
	JSON_PARSE_INT(body, SEGSTAT_STATUSDESC, item, RMSEG_INBUILDHOST->StatusDesc)
	JSON_PARSE_INT(header, SEGSTAT_RMSTARTTIMESTAMP, item, RMSEG_INBUILDHOST->RMStartTimestamp)
	JSON_PARSE_INT(info, SEGSTAT_PORT, item, RMSEG_INBUILDHOST->Info.port)
	JSON_PARSE_INT(info, SEGSTAT_ID, item, RMSEG_INBUILDHOST->Info.ID)
	JSON_PARSE_INT(info, SEGSTAT_MASTER, item, RMSEG_INBUILDHOST->Info.master)
	JSON_PARSE_INT(info, SEGSTAT_STANDBY, item, RMSEG_INBUILDHOST->Info.standby)
	JSON_PARSE_INT(info, SEGSTAT_ALIVE, item, RMSEG_INBUILDHOST->Info.alive)

	jumpforwardSelfMaintainBuffer(&localsegstat, sizeof(SegStatData));

	/* Deal with ip address. */
	JSON_PARSE_INT(info, SEGSTAT_HOSTADDRCOUNT, item, RMSEG_INBUILDHOST->Info.HostAddrCount)

	/* Compute address offset */
	uint16_t addroffset = sizeof(SegInfoData) + sizeof(uint32_t)
					* (((RMSEG_INBUILDHOST->Info.HostAddrCount + 1) >> 1) << 1);
	uint16_t addrattr = 0;
	addrattr |= HOST_ADDRESS_CONTENT_STRING;

	RMSEG_INBUILDHOST->Info.AddressAttributeOffset = sizeof(SegInfoData);
	RMSEG_INBUILDHOST->Info.AddressContentOffset = addroffset;

	json_object_object_get_ex(info, SEGSTAT_ADDRARRAY, &item);
	if (NULL == item)
		goto failed;
	for (int i = 0; i < json_object_array_length(item); i++) {
		char *addr = json_object_get_string(json_object_array_get_idx(item, i));
		HostAddress newaddr = NULL;
		newaddr = createHostAddressAsStringFromIPV4AddressStr(PCONTEXT, addr);
		appendSMBVar(&localsegstat, addroffset);
		appendSMBVar(&localsegstat, addrattr);
		addroffset += newaddr->AddressSize;
	}
	appendSelfMaintainBufferTill64bitAligned(&localsegstat);

	/* Add address content*/
	/* If there are more than one ip address and the first ip is 127.0.0.1, then set needToExchangeIP to true,
	 *  put the second ip to the first place. If there is only one ip address, no need to change.*/
	bool needToExchangeIP = false;
	for (int i = 0; i < json_object_array_length(item); i++) {
		char * addr = json_object_get_string(json_object_array_get_idx(item, i));
		HostAddress newaddr = NULL;
		newaddr = createHostAddressAsStringFromIPV4AddressStr(PCONTEXT, addr);
		if (i == 0 && strcmp(addr, "127.0.0.1") == 0 && json_object_array_length(item) != 1) {
			needToExchangeIP = true;
		}
		if (i == 1 && needToExchangeIP == true) {
			elog(LOG, "needToExchange is true && i==1");
			strncpy(fts_client_ip, addr, fts_client_ip_len);
			elog(LOG, "fts_client_ip is %s", fts_client_ip);
		}
		appendSelfMaintainBuffer(&localsegstat, newaddr->Address, newaddr->AddressSize);
		elog(LOG, "addr of addressArray is %s, newaddr which is convert to HostAddress is %s, newaddr size is %d", addr, newaddr->Address, newaddr->AddressSize);
	}

	/* Check if the ip address is convert correct */
	for (int i = 0; i < RMSEG_INBUILDHOST->Info.HostAddrCount; i++) {
		uint16_t __MAYBE_UNUSED attr = GET_SEGINFO_ADDR_ATTR_AT(
				&RMSEG_INBUILDHOST->Info, i);
		Assert(IS_SEGINFO_ADDR_STR(attr));
		AddressString straddr = NULL;
		getSegInfoHostAddrStr(&RMSEG_INBUILDHOST->Info, i, &straddr);
		elog(LOG, "Check ip address convert exactly : i is %d, attr is %d ,adrres is %s",i , attr, straddr->Address);
	}

	/* Deal with host name */
	/* Get the hostname values before doing the json parse. */
	int HostNameOffset = 0;
	int HostNameLen = 0;
	char * hostname = NULL;
	JSON_PARSE_INT(info, SEGSTAT_HOSTNAMEOFFSET, item, HostNameOffset)
	JSON_PARSE_INT(info, SEGSTAT_HOSTNAMELEN, item, HostNameLen)
	JSON_PARSE_STR(info, SEGSTAT_HOSTNAME, item, hostname)

	/* Get the hostname values by computing .*/
	RMSEG_INBUILDHOST->Info.HostNameOffset = localsegstat.Cursor + 1 - offsetof(SegStatData, Info);
	appendSelfMaintainBuffer(&localsegstat, hostname, strlen(hostname)+1);
	char * hostName = ((char *)(&RMSEG_INBUILDHOST->Info) + (RMSEG_INBUILDHOST->Info.HostNameOffset));

	appendSelfMaintainBufferTill64bitAligned(&localsegstat);
	RMSEG_INBUILDHOST->Info.HostNameLen = strlen(hostName);

	elog(LOG, "HostNameOffset before json parse is %d, after parse is %d", HostNameOffset, RMSEG_INBUILDHOST->Info.HostNameOffset);
	elog(LOG, "hostname before parse is %s, hostname after parse is %s", hostname, hostName);
	elog(LOG, "HostName len before parse is %d, after parse is %d",HostNameLen, RMSEG_INBUILDHOST->Info.HostNameLen);

	/*Deal with GRM host/rack. */
	JSON_PARSE_INT(info, SEGSTAT_GRMHOSTNAMEOFFSET, item, RMSEG_INBUILDHOST->Info.GRMHostNameOffset)
	JSON_PARSE_INT(info, SEGSTAT_GRMHOSTNAMELEN, item, RMSEG_INBUILDHOST->Info.GRMHostNameLen)
	JSON_PARSE_INT(info, SEGSTAT_GRMRACKNAMEOFFSET, item, RMSEG_INBUILDHOST->Info.GRMRackNameOffset)
	JSON_PARSE_INT(info, SEGSTAT_GRMRACKNAMELEN, item, RMSEG_INBUILDHOST->Info.GRMRackNameLen)

	if (RMSEG_INBUILDHOST->FailedTmpDirNum == 0) {
		RMSEG_INBUILDHOST->Info.FailedTmpDirOffset = 0;
		RMSEG_INBUILDHOST->Info.FailedTmpDirLen = 0;
	} else {
		JSON_PARSE_INT(info, SEGSTAT_FAILEDTMPDIROFFSET, item, RMSEG_INBUILDHOST->Info.FailedTmpDirOffset)
		JSON_PARSE_INT(info, SEGSTAT_FAILEDTMPDIRLEN, item, RMSEG_INBUILDHOST->Info.FailedTmpDirLen)
		char * failedTmpDir = NULL;
		JSON_PARSE_STR(info, SEGSTAT_FAILEDTMPDIR, item, failedTmpDir)
		int FailedTmpDirOffset = localsegstat.Cursor + 1 - offsetof(SegStatData, Info);
		elog(LOG, "FailedTmpDirOffset is %d, %d", RMSEG_INBUILDHOST->Info.FailedTmpDirOffset, FailedTmpDirOffset);
		appendSelfMaintainBuffer(&localsegstat, failedTmpDir, strlen(failedTmpDir)+1);
		appendSelfMaintainBufferTill64bitAligned(&localsegstat);
	}

	/* Get total size and check. */
	int Size = 0;
	JSON_PARSE_INT(info, SEGSTAT_SIZE, item, Size)
	RMSEG_INBUILDHOST->Info.Size = localsegstat.Cursor + 1 - offsetof(SegStatData, Info);
	elog(LOG, "localsegstat Info.Size is : %d, %d, %d, %d", RMSEG_INBUILDHOST->Info.Size, Size, localsegstat.Size, localsegstat.Cursor);

	SegStat segstat = NULL;
	segstat = palloc0(localsegstat.Cursor + 1);
	memcpy(segstat, localsegstat.Buffer, localsegstat.Cursor + 1);
	SelfMaintainBufferData machinereport;
	initializeSelfMaintainBuffer(&machinereport, PCONTEXT);
	generateSegStatReport(segstat, &machinereport);
	elog(LOG, "machinereport is %s",machinereport.Buffer);
	destroySelfMaintainBuffer(&machinereport);
	destroySelfMaintainBuffer(&localsegstat);

	return segstat;

failed:
	elog(WARNING, "Wrong json parser");
	return NULL;
}

/*
 * Add a new IP and a new host name to SegStat.
 */
SegStat addNewIPToSegStat(SegStat segstat, char* fts_client_ip, uint32_t fts_client_ip_len, char* fts_client_host)
{
	SelfMaintainBufferData newseginfo;
	initializeSelfMaintainBuffer(&newseginfo, PCONTEXT);
	prepareSelfMaintainBuffer(&newseginfo, sizeof(SegInfoData), false);
	memcpy(SMBUFF_CONTENT(&(newseginfo)), &segstat->Info, sizeof(SegInfoData));
	jumpforwardSelfMaintainBuffer(&newseginfo, sizeof(SegInfoData));

	uint16_t addroffset = sizeof(SegInfoData)
			+ sizeof(uint32_t)
					* (((segstat->Info.HostAddrCount + 1 + 1) >> 1) << 1);
	uint16_t addrattr = HOST_ADDRESS_CONTENT_STRING;

	SegInfo newseginfoptr = SMBUFF_HEAD(SegInfo, &newseginfo);
	newseginfoptr->AddressAttributeOffset = sizeof(SegInfoData);
	newseginfoptr->AddressContentOffset = addroffset;
	newseginfoptr->HostAddrCount = segstat->Info.HostAddrCount + 1;

	uint32_t addContentOffset = addroffset;

	appendSMBVar(&newseginfo, addroffset);
	appendSMBVar(&newseginfo, addrattr);

	elog(LOG, "resource manager received IMAlive message, this segment's IP "
	"address count: %d",segstat->Info.HostAddrCount);

	/* iterate all the offset/attribute in machineIdData from client */
	for (int i = 0; i < segstat->Info.HostAddrCount; i++) {
		/*
		 * Adjust address offset by counting the size of one AddressString.
		 * Adding new address attribute and offset content can also causing more
		 * space enlarged. We have to count it.
		 */
		addroffset = *(uint16_t *) ((char *) &segstat->Info
				+ segstat->Info.AddressAttributeOffset + i * sizeof(uint32_t));
		addroffset += __SIZE_ALIGN64(sizeof(uint32_t) + fts_client_ip_len + 1)
				+ (addContentOffset - segstat->Info.AddressContentOffset);

		appendSMBVar(&newseginfo, addroffset);

		addrattr = *(uint16_t *) ((char *) &segstat->Info
				+ segstat->Info.AddressAttributeOffset + i * sizeof(uint32_t)
				+ sizeof(uint16_t));
		/* No need to adjust the value. */
		appendSMBVar(&newseginfo, addrattr);
	}
	/* We may have to add '\0' pads to make the block of address offset and
	* attribute 64-bit aligned. */
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);
	/* Put the connection's client ip into the first position */
	appendSMBVar(&newseginfo, fts_client_ip_len);
	appendSMBStr(&newseginfo, fts_client_ip);
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	elog(LOG, "resource manager received IMAlive message, "
	"this segment's IP address: %s\n",fts_client_ip);

	appendSelfMaintainBuffer(&newseginfo,
			(char *) &segstat->Info + segstat->Info.AddressContentOffset,
			segstat->Info.HostNameOffset - segstat->Info.AddressContentOffset);

	newseginfoptr = SMBUFF_HEAD(SegInfo, &(newseginfo));
	newseginfoptr->HostNameOffset = getSMBContentSize(&newseginfo);
	appendSMBStr(&newseginfo, fts_client_host);
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	newseginfoptr = SMBUFF_HEAD(SegInfo, &(newseginfo));
	newseginfoptr->HostNameLen = strlen(fts_client_host);
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	/* fill in failed temporary directory string */
	if (segstat->Info.FailedTmpDirLen != 0) {
		newseginfoptr->FailedTmpDirOffset = getSMBContentSize(&newseginfo);
		newseginfoptr->FailedTmpDirLen = strlen(
				GET_SEGINFO_FAILEDTMPDIR(&segstat->Info));
		appendSMBStr(&newseginfo, GET_SEGINFO_FAILEDTMPDIR(&segstat->Info));
		elog(RMLOG, "resource manager received IMAlive message, "
		"failed temporary directory:%s",
		GET_SEGINFO_FAILEDTMPDIR(&segstat->Info));
		appendSelfMaintainBufferTill64bitAligned(&newseginfo);
	} else {
		newseginfoptr->FailedTmpDirOffset = 0;
		newseginfoptr->FailedTmpDirLen = 0;
	}

	newseginfoptr->Size = getSMBContentSize(&newseginfo);
	newseginfoptr->GRMHostNameLen = 0;
	newseginfoptr->GRMHostNameOffset = 0;
	newseginfoptr->GRMRackNameLen = 0;
	newseginfoptr->GRMRackNameOffset = 0;

	elog(RMLOG, "resource manager received IMAlive message, "
	"this segment's hostname: %s\n", fts_client_host);

	SegStat newsegstat = (SegStat) rm_palloc0(PCONTEXT,
			offsetof(SegStatData, Info) + getSMBContentSize(&newseginfo));
	memcpy((char *) newsegstat, segstat, offsetof(SegStatData, Info));
	memcpy((char *) newsegstat + offsetof(SegStatData, Info),
			SMBUFF_CONTENT(&newseginfo), getSMBContentSize(&newseginfo));

	destroySelfMaintainBuffer(&newseginfo);

	newsegstat->Info.ID = SEGSTAT_ID_INVALID;
	newsegstat->StatusDesc = 0;

	return newsegstat;
}

/*
 * Handle Etcd I AM ALIVE request.
 */
bool handleRMSEGRequestIMAliveEtcd(char *key, char *value, bool isNewHost)
{
	/*Segment to delete from etcd server.*/
	char deletedHostName[100] = {0};
	char client_host[100] = {0};
	int dirlen = strlen(rm_etcd_server_dir);
	int keylen = strlen(key);
	if (strcmp(value, "") == 0) {
		strncpy(deletedHostName, key + dirlen, keylen);
		elog(LOG, "deletedHostName is %s", deletedHostName);
		deleteNodesForEtcd(deletedHostName);
		return false;
	}

	/* Exchange ip address if the first ip is 127.0.0.1 */
	char fts_client_ip[100] = {0};
	//struct hostent* fts_client_host = NULL;
	uint32_t fts_client_ip_len = 0;
	//struct in_addr fts_client_addr;
	SegStat segstat = NULL;
	/* Parse json string from etcd server to segstat. */
	segstat = handleEtcdJsonParse(key, value, fts_client_ip, 100);
	strncpy(client_host, key + dirlen, keylen);
	fts_client_ip_len = strlen(fts_client_ip);
	if (segstat == NULL)
    {
        elog(LOG, "Fail to parse segstat, the segstat is NULL");
		return false;
    }
	elog(LOG," fts_client_ip is %s, strlen(fts_client_ip) is %d", fts_client_ip, strlen(fts_client_ip));
	/*
	if(strlen(fts_client_ip) != 0)
	{
		fts_client_ip_len = strlen(fts_client_ip);
		inet_aton(fts_client_ip, &fts_client_addr);
		//fts_client_host = gethostbyaddr(&fts_client_addr, 4, AF_INET);
		//elog(LOG, "fts_client_ip is %s, fts_client_host is %s, fts_client_ip_len is %d", fts_client_ip, fts_client_host->h_name, fts_client_ip_len);
		if (fts_client_host == NULL) {
			strncpy(client_host, key + dirlen, keylen);
			elog(WARNING, "failed to reverse DNS lookup for ip %s.", fts_client_ip);
		}
		else
			strcpy(client_host, fts_client_host->h_name);
	}
	*/

	/* Need to add fts_client_ip to the first place of the ip address array. */
	bool capstatchanged = false;
	if (0 != strlen(client_host))
	{
		SegStat newsegstat = NULL;
		newsegstat = addNewIPToSegStat(segstat, fts_client_ip, fts_client_ip_len, client_host);

		if (PRESPOOL->AvailNodeCount < cluster_size) {
			if (addHAWQSegWithSegStat(newsegstat, &capstatchanged) != FUNC_RETURN_OK) {
				rm_pfree(PCONTEXT, newsegstat);
			}
			if (isNewHost) {
				char addHostName[100];
				strncpy(addHostName, key + dirlen, keylen);
				addNodesForEtcd(addHostName);
				return true;
			}
		}
		if (capstatchanged) {
			/* Refresh resource queue capacities. */
			refreshResourceQueueCapacity(false);
			refreshActualMinGRMContainerPerSeg();
			/* Recalculate all memory/core ratio instances' limits. */
			refreshMemoryCoreRatioLimits();
			/* Refresh memory/core ratio level water mark. */
			refreshMemoryCoreRatioWaterMark();
		}
    }
    else
	{
		segstat->Info.ID = SEGSTAT_ID_INVALID;
		segstat->StatusDesc = 0;

		if (PRESPOOL->AvailNodeCount < cluster_size) {
			if (addHAWQSegWithSegStat(segstat, &capstatchanged)
					!= FUNC_RETURN_OK) {
				rm_pfree(PCONTEXT, segstat);
			}
			if (isNewHost) {
				char addHostName[100];
				strncpy(addHostName, key + dirlen, keylen);
				addNodesForEtcd(addHostName);
				return true;
			}
		}
		if (capstatchanged) {
			/* Refresh resource queue capacities. */
			refreshResourceQueueCapacity(false);
			refreshActualMinGRMContainerPerSeg();
			/* Recalculate all memory/core ratio instances' limits. */
			refreshMemoryCoreRatioLimits();
			/* Refresh memory/core ratio level water mark. */
			refreshMemoryCoreRatioWaterMark();
		}
		
	}

	return true;
}

/*
 * Handle I AM ALIVE request.
 */
bool handleRMSEGRequestIMAlive(void **arg)
{
	ConnectionTrack conntrack = (ConnectionTrack)(*arg);
	elog(RMLOG, "resource manager receives segment heart-beat information.");

	SelfMaintainBufferData machinereport;
	initializeSelfMaintainBuffer(&machinereport,PCONTEXT);
	SegStat segstat = (SegStat)(SMBUFF_CONTENT(&(conntrack->MessageBuff)) +
								sizeof(RPCRequestHeadIMAliveData));

	generateSegStatReport(segstat, &machinereport);

	elog(RMLOG, "resource manager received segment machine information, %s",
				SMBUFF_CONTENT(&machinereport));
	destroySelfMaintainBuffer(&machinereport);

	/* Get hostname and ip address from the connection's sockaddr */
	char*    		fts_client_ip     = NULL;
	uint32_t 		fts_client_ip_len = 0;
	struct hostent* fts_client_host   = NULL;
	struct in_addr 	fts_client_addr;

	Assert(conntrack->CommBuffer != NULL);
	fts_client_ip = conntrack->CommBuffer->ClientAddrDotStr;
	fts_client_ip_len = strlen(fts_client_ip);
	inet_aton(fts_client_ip, &fts_client_addr);
	fts_client_host = gethostbyaddr(&fts_client_addr, 4, AF_INET);
	if (fts_client_host == NULL)
	{
		elog(WARNING, "failed to reverse DNS lookup for ip %s.", fts_client_ip);
		return true;
	}

	/* Get the received machine id instance start address. */
	SegInfo fts_client_seginfo = (SegInfo)(SMBUFF_CONTENT(&(conntrack->MessageBuff)) +
										   sizeof(RPCRequestHeadIMAliveData) +
										   offsetof(SegStatData, Info));

	/* Build new machine id with inserted ip address and modified host name. */
	SelfMaintainBufferData newseginfo;
	initializeSelfMaintainBuffer(&newseginfo, PCONTEXT);

	/* Copy machine id header. */
	prepareSelfMaintainBuffer(&newseginfo, sizeof(SegInfoData), false);
	memcpy(SMBUFF_CONTENT(&(newseginfo)),
		   fts_client_seginfo, sizeof(SegInfoData));
	jumpforwardSelfMaintainBuffer(&newseginfo, sizeof(SegInfoData));

	/* Put client ip address's offset and attribute */
	uint16_t addroffset = sizeof(SegInfoData) +
						  sizeof(uint32_t) *
						  (((fts_client_seginfo->HostAddrCount + 1 + 1) >> 1) << 1);

	uint16_t addrattr = HOST_ADDRESS_CONTENT_STRING;

	SegInfo newseginfoptr = SMBUFF_HEAD(SegInfo, &newseginfo);
	newseginfoptr->AddressAttributeOffset = sizeof(SegInfoData);
	newseginfoptr->AddressContentOffset   = addroffset;
	newseginfoptr->HostAddrCount 		  = fts_client_seginfo->HostAddrCount + 1;

	uint32_t addContentOffset= addroffset;

	appendSMBVar(&newseginfo,addroffset);
	appendSMBVar(&newseginfo,addrattr);

	elog(RMLOG, "resource manager received IMAlive message, this segment's IP "
				"address count: %d",
				fts_client_seginfo->HostAddrCount);

	/* iterate all the offset/attribute in machineIdData from client */
	for (int i = 0; i < fts_client_seginfo->HostAddrCount; i++) {

		/* Read old address offset data. */
		addroffset = *(uint16_t *)((char *)fts_client_seginfo +
								   fts_client_seginfo->AddressAttributeOffset +
								   i*sizeof(uint32_t));
		/*
		 * Adjust address offset by counting the size of one AddressString.
		 * Adding new address attribute and offset content can also causing more
		 * space enlarged. We have to count it.
		 */
		addroffset += __SIZE_ALIGN64(sizeof(uint32_t) +
									 fts_client_ip_len + 1) +
					  (addContentOffset - fts_client_seginfo->AddressContentOffset);

		appendSMBVar(&newseginfo,addroffset);

		/* Read old address attribute data. */
		addrattr = *(uint16_t *)((char *)fts_client_seginfo +
								 fts_client_seginfo->AddressAttributeOffset +
								 i*sizeof(uint32_t) +
								 sizeof(uint16_t));
		/* No need to adjust the item. */
		appendSMBVar(&newseginfo,addrattr);
	}

	/* We may have to add '\0' pads to make the block of address offset and
	 * attribute 64-bit aligned. */
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	/* Put the connection's client ip into the first position */
	appendSMBVar(&newseginfo,fts_client_ip_len);
	appendSMBStr(&newseginfo,fts_client_ip);
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	elog(RMLOG, "resource manager received IMAlive message, "
				"this segment's IP address: %s\n",
				fts_client_ip);

	/* Put other ip addresses' content directly. */
	appendSelfMaintainBuffer(&newseginfo,
							 (char *)fts_client_seginfo +
							 	 	 fts_client_seginfo->AddressContentOffset,
							 fts_client_seginfo->HostNameOffset -
							 fts_client_seginfo->AddressContentOffset);

	/* fill in hostname */
	newseginfoptr = SMBUFF_HEAD(SegInfo, &(newseginfo));
	newseginfoptr->HostNameOffset = getSMBContentSize(&newseginfo);
	appendSMBStr(&newseginfo,fts_client_host->h_name);
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	newseginfoptr = SMBUFF_HEAD(SegInfo, &(newseginfo));
	newseginfoptr->HostNameLen = strlen(fts_client_host->h_name);
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	/* fill in failed temporary directory string */
	if (fts_client_seginfo->FailedTmpDirLen != 0)
	{
		newseginfoptr->FailedTmpDirOffset = getSMBContentSize(&newseginfo);
		newseginfoptr->FailedTmpDirLen = strlen(GET_SEGINFO_FAILEDTMPDIR(fts_client_seginfo));
		appendSMBStr(&newseginfo, GET_SEGINFO_FAILEDTMPDIR(fts_client_seginfo));
		elog(RMLOG, "resource manager received IMAlive message, "
					"failed temporary directory:%s",
					GET_SEGINFO_FAILEDTMPDIR(fts_client_seginfo));
		appendSelfMaintainBufferTill64bitAligned(&newseginfo);
	}
	else
	{
		newseginfoptr->FailedTmpDirOffset = 0;
		newseginfoptr->FailedTmpDirLen = 0;
	}

	newseginfoptr->Size      = getSMBContentSize(&newseginfo);
	/* reported by segment, set GRM host/rack as NULL */
	newseginfoptr->GRMHostNameLen = 0;
	newseginfoptr->GRMHostNameOffset = 0;
	newseginfoptr->GRMRackNameLen = 0;
	newseginfoptr->GRMRackNameOffset = 0;

	elog(RMLOG, "resource manager received IMAlive message, "
				"this segment's hostname: %s\n",
				fts_client_host->h_name);

	/* build segment status information instance and add to resource pool */
	SegStat newsegstat = (SegStat)
						 rm_palloc0(PCONTEXT,
								  	offsetof(SegStatData, Info) +
									getSMBContentSize(&newseginfo));
	/* Copy old segment status information. */
	memcpy((char *)newsegstat, segstat, offsetof(SegStatData, Info));
	/* Copy new segment info information. */
	memcpy((char *)newsegstat + offsetof(SegStatData, Info),
		   SMBUFF_CONTENT(&newseginfo),
		   getSMBContentSize(&newseginfo));

	destroySelfMaintainBuffer(&newseginfo);

	newsegstat->Info.ID 		 = SEGSTAT_ID_INVALID;

	RPCRequestHeadIMAlive header = SMBUFF_HEAD(RPCRequestHeadIMAlive,
												&(conntrack->MessageBuff));
	newsegstat->FailedTmpDirNum  = header->TmpDirBrokenCount;
	newsegstat->RMStartTimestamp = header->RMStartTimestamp;
	newsegstat->StatusDesc = 0;

	bool capstatchanged = false;
	if ( addHAWQSegWithSegStat(newsegstat, &capstatchanged) != FUNC_RETURN_OK )
	{
		/* Should be a duplicate host. */
		rm_pfree(PCONTEXT, newsegstat);
	}

	if ( capstatchanged )
	{
		/* Refresh resource queue capacities. */
		refreshResourceQueueCapacity(false);
		refreshActualMinGRMContainerPerSeg();
		/* Recalculate all memory/core ratio instances' limits. */
		refreshMemoryCoreRatioLimits();
		/* Refresh memory/core ratio level water mark. */
		refreshMemoryCoreRatioWaterMark();
	}
        
	/* Send the response. */
	RPCResponseIMAliveData response;
	response.Result   = FUNC_RETURN_OK;
	response.Reserved = 0;
	buildResponseIntoConnTrack(conntrack,
						 	   (char *)&response,
							   sizeof(response),
							   conntrack->MessageMark1,
							   conntrack->MessageMark2,
							   RESPONSE_RM_IMALIVE);

	elog(DEBUG3, "resource manager accepted segment machine.");

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

bool handleRMRequestAcquireResourceQuota(void **arg)
{
	static char		 errorbuf[ERRORMESSAGE_SIZE];
	int      		 res		= FUNC_RETURN_OK;
	ConnectionTrack  conntrack  = (ConnectionTrack)(*arg);
	bool			 exist		= false;
	uint64_t		 reqtime	= gettime_microsec();
	/* If we run in YARN mode, we expect that we should try to get at least one
	 * available segment, and this requires at least once global resource manager
	 * cluster report returned.
	 */
	if ( reqtime - DRMGlobalInstance->ResourceManagerStartTime <=
		 rm_nocluster_timeout * 1000000LL &&
		 PRESPOOL->RBClusterReportCounter == 0 )
	{
		elog(DEBUG3, "resource manager defers the resource request.");
		return false;
	}

	/*
	 * If resource queue has no concrete capacity set yet, no need to handle
	 * the request.
	 */
	if ( PQUEMGR->RootTrack->QueueInfo->ClusterMemoryMB <= 0 )
	{
		elog(DEBUG3, "resource manager defers the resource request because the "
					 "resource queues have no valid resource capacities yet.");
		return false;
	}

	Assert(PRESPOOL->SlavesHostCount > 0);
	int rejectlimit = ceil(PRESPOOL->SlavesHostCount * rm_rejectrequest_nseg_limit);
	int unavailcount = PRESPOOL->SlavesHostCount - PRESPOOL->AvailNodeCount;
	if ( unavailcount > rejectlimit )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "%d of %d segments %s unavailable, exceeds %.1f%% defined in "
				 "GUC hawq_rm_rejectrequest_nseg_limit. The resource quota "
				 "request is rejected.",
				 unavailcount,
				 PRESPOOL->SlavesHostCount,
				 unavailcount == 1 ? "is" : "are",
				 rm_rejectrequest_nseg_limit*100.0);
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		res = RESOURCEPOOL_TOO_MANY_UAVAILABLE_HOST;
		goto errorexit;
	}

	RPCRequestHeadAcquireResourceQuotaFromRMByOID request =
		SMBUFF_HEAD(RPCRequestHeadAcquireResourceQuotaFromRMByOID,
					&(conntrack->MessageBuff));

	/* Get user name from oid. */
	UserInfo reguser = getUserByUserOID(request->UseridOid, &exist);
	if ( !exist )
	{
		res = RESQUEMGR_NO_USERID;
		snprintf(errorbuf, sizeof(errorbuf),
				 "user oid " INT64_FORMAT "does not exist",
				 request->UseridOid);
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		goto errorexit;
	}
	else
	{
		/* Set user id string into connection track. */
		strncpy(conntrack->UserID, reguser->Name, sizeof(conntrack->UserID)-1);
	}

	conntrack->MaxSegCountFixed = request->MaxSegCountFix;
	conntrack->MinSegCountFixed = request->MinSegCountFix;
	conntrack->VSegLimitPerSeg	= request->VSegLimitPerSeg;
	conntrack->VSegLimit		= request->VSegLimit;
	conntrack->StatVSegMemoryMB	= request->StatVSegMemoryMB;
	conntrack->StatNVSeg		= request->StatNVSeg;

	elog(RMLOG, "ConnID %d. User "INT64_FORMAT" acquires query resource quota. "
			  	"Expect %d vseg (MIN %d). "
				"Each segment has maximum %d vseg. "
				"Query has maximum %d vseg. "
				"Statement quota %d MB x %d vseg",
				conntrack->ConnID,
				request->UseridOid,
				request->MaxSegCountFix,
				request->MinSegCountFix,
				request->VSegLimitPerSeg,
				request->VSegLimit,
				request->StatVSegMemoryMB,
				request->StatNVSeg);

	if ( conntrack->StatNVSeg > 0 )
	{
		elog(LOG, "ConnID %d. Statement level resource quota is active. "
				  "Expect resource ( %d MB ) x %d.",
				  conntrack->ConnID,
				  conntrack->StatVSegMemoryMB,
				  conntrack->StatNVSeg);
	}

	res = acquireResourceQuotaFromResQueMgr(conntrack, errorbuf, sizeof(errorbuf));

	if ( res == FUNC_RETURN_OK )
	{
		RPCResponseHeadAcquireResourceQuotaFromRMByOIDData response;

		DynResourceQueueTrack queuetrack = getQueueTrackByQueueOID(reguser->QueueOID);
		if ( queuetrack != NULL )
		{
			memcpy(response.QueueName,
				   queuetrack->QueueInfo->Name,
				   sizeof(response.QueueName));
		}
		else {
			response.QueueName[0]='\0';
		}

		response.Reserved1   = 0;
		response.Result      = res;
		response.SegCore     = conntrack->SegCore;
		response.SegMemoryMB = conntrack->SegMemoryMB;
		response.SegNum	     = conntrack->SegNum;
		response.SegNumMin   = conntrack->SegNumMin;
		response.Reserved2   = 0;

		buildResponseIntoConnTrack( conntrack,
									(char *)&response,
									sizeof(response),
									conntrack->MessageMark1,
									conntrack->MessageMark2,
									RESPONSE_QD_ACQUIRE_RESOURCE_QUOTA);
		conntrack->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
		return true;
	}
	else
	{
		elog(WARNING, "%s", errorbuf);
	}
errorexit:
	{
		SelfMaintainBufferData responsedata;
		initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

		buildResponseIntoConnTrack(conntrack,
								   SMBUFF_CONTENT(&responsedata),
								   getSMBContentSize(&responsedata),
								   conntrack->MessageMark1,
								   conntrack->MessageMark2,
								   RESPONSE_QD_ACQUIRE_RESOURCE_QUOTA);
		destroySelfMaintainBuffer(&responsedata);
		conntrack->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
	}
	return true;
}

bool handleRMRequestRefreshResource(void **arg)
{
	int      		 res		= FUNC_RETURN_OK;
	ConnectionTrack  conntrack  = (ConnectionTrack)(*arg);
	ConnectionTrack  oldct		= NULL;
	uint64_t		 curmsec    = gettime_microsec();

	RPCRequestHeadRefreshResourceHeartBeat request =
		SMBUFF_HEAD(RPCRequestHeadRefreshResourceHeartBeat,
					&(conntrack->MessageBuff));

	uint32_t *connids = (uint32_t *)
						(SMBUFF_CONTENT(&(conntrack->MessageBuff)) +
						 sizeof(RPCRequestHeadRefreshResourceHeartBeatData));

	elog(DEBUG3, "resource manager refreshes %d ConnIDs.", request->ConnIDCount);

	for ( int i = 0 ; i < request->ConnIDCount ; ++i )
	{
		/* Find connection track identified by ConnID */
		res = getInUseConnectionTrack(connids[i], &oldct);
		if ( res == FUNC_RETURN_OK )
		{
			oldct->LastActTime = curmsec;
			elog(RMLOG, "refreshed resource context connection id %d", connids[i]);
		}
		else
		{
			elog(WARNING, "cannot find resource context connection id %d for "
						  "resource refreshing.",
					      connids[i]);
		}
	}

	/* Temporarily, we ignore the wrong conn id inputs. */
	RPCResponseRefreshResourceHeartBeatData response;
	response.Result   = FUNC_RETURN_OK;
	response.Reserved = 0;

	buildResponseIntoConnTrack( conntrack,
								(char *)&response,
								sizeof(response),
								conntrack->MessageMark1,
								conntrack->MessageMark2,
								RESPONSE_QD_REFRESH_RESOURCE);
	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK
	return true;
}

bool handleRMRequestSegmentIsDown(void **arg)
{
	int      		 res		 = FUNC_RETURN_OK;
	ConnectionTrack  conntrack   = (ConnectionTrack)(*arg);
	/* Get host name that is down. */
	char 			*hostname    = SMBUFF_CONTENT(&(conntrack->MessageBuff));
	int				 hostnamelen = 0;
	int32_t		 	 segid       = SEGSTAT_ID_INVALID;

	while( (hostname - SMBUFF_CONTENT(&(conntrack->MessageBuff)) <
			getSMBContentSize(&(conntrack->MessageBuff))) &&
			*hostname != '\0' )
	{
		hostnamelen = strlen(hostname);
		res = getSegIDByHostName(hostname, hostnamelen, &segid);
		if ( res == FUNC_RETURN_OK )
		{
			/* Get resource info of the expected host. */
			SegResource segres = getSegResource(segid);
			Assert( segres != NULL );

			if ( !IS_SEGSTAT_FTSAVAILABLE(segres->Stat) )
			{
				elog(WARNING, "resource manager does not probe the status of "
							  "host %s because it is down already.",
							  hostname);
			}
			else if ( segres->RUAlivePending )
			{
				elog(LOG, "resource manager does not probe the status of host %s "
						  "because it is in RUAlive pending status already.",
						  hostname);
			}
			else
			{
				elog(RMLOG, "resource manager probes the status of host %s by "
							"sending RUAlive request.",
							hostname);

				res = sendRUAlive(hostname);
				/* IN THIS CASE, the segment is considered as down. */
				if (res != FUNC_RETURN_OK)
				{
					/*----------------------------------------------------------
					 * This call makes resource manager able to adjust queue and
					 * mem/core trackers' capacity.
					 *----------------------------------------------------------
					 */
					setSegResHAWQAvailability(segres,
											  RESOURCE_SEG_STATUS_UNAVAILABLE);

					/* Make resource pool remove unused containers */
					returnAllGRMResourceFromSegment(segres);
					/* Set the host down in gp_segment_configuration table */
					segres->Stat->StatusDesc |= SEG_STATUS_FAILED_PROBING_SEGMENT;
					if (Gp_role != GP_ROLE_UTILITY)
					{
						SimpStringPtr description = build_segment_status_description(segres->Stat);
						update_segment_status(segres->Stat->Info.ID + REGISTRATION_ORDER_OFFSET,
											  SEGMENT_STATUS_DOWN,
											  (description->Len > 0)?description->Str:"");
						add_segment_history_row(segres->Stat->Info.ID + REGISTRATION_ORDER_OFFSET,
												hostname,
												description->Str);

						freeSimpleStringContent(description);
						rm_pfree(PCONTEXT, description);
					}

					/* Set the host down. */
					elog(LOG, "resource manager sets host %s from up to down "
							  "due to not reaching host.", hostname);
				}
				else
				{
					elog(RMLOG, "resource manager triggered RUAlive request to "
								"host %s.",
								hostname);
				}
			}
		}
		else {
			elog(WARNING, "resource manager cannot find host %s to check status, "
						  "skip it.",
						  hostname);
		}

		hostname = hostname + strlen(hostname) + 1; /* Try next */
	}

	refreshResourceQueueCapacity(false);
	refreshActualMinGRMContainerPerSeg();

	RPCResponseSegmentIsDownData response;
	response.Result   = res;
	response.Reserved = 0;
	buildResponseIntoConnTrack( conntrack,
								(char *)&response,
								sizeof(response),
								conntrack->MessageMark1,
								conntrack->MessageMark2,
								RESPONSE_QD_SEGMENT_ISDOWN);
	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

bool handleRMRequestDumpResQueueStatus(void **arg)
{
    ConnectionTrack conntrack   = (ConnectionTrack)(*arg);
    RPCResponseResQueueStatus   response = NULL;
    int responseLen = sizeof(RPCResponseResQueueStatusData) 
                    + sizeof(ResQueueStatusData) * (list_length(PQUEMGR->Queues) - 1);

    response = (RPCResponseResQueueStatus)palloc(responseLen);

    response->Result 	= FUNC_RETURN_OK;
    response->queuenum 	= list_length(PQUEMGR->Queues);

    int i = 0;
    ListCell *cell = NULL;
    foreach(cell, PQUEMGR->Queues)
    {
    	DynResourceQueueTrack quetrack = lfirst(cell);

        sprintf(response->queuedata[i].name, "%s", quetrack->QueueInfo->Name);
        response->queuedata[i].segmem     = quetrack->QueueInfo->SegResourceQuotaMemoryMB;
        response->queuedata[i].segcore    = quetrack->QueueInfo->SegResourceQuotaVCore;
        response->queuedata[i].segsize    = quetrack->ClusterSegNumber;
        response->queuedata[i].segsizemax = quetrack->ClusterSegNumberMax;
        response->queuedata[i].inusemem   = quetrack->TotalUsed.MemoryMB;
        response->queuedata[i].inusecore  = quetrack->TotalUsed.Core;
        response->queuedata[i].holders    = quetrack->NumOfRunningQueries;
        response->queuedata[i].waiters    = quetrack->QueryResRequests.NodeCount;

        /* Generate if resource queue paused dispatching resource. */
        if ( quetrack->troubledByFragment )
        {
        	response->queuedata[i].pausedispatch = 'R';
        }
        else if ( quetrack->pauseAllocation )
        {
        	response->queuedata[i].pausedispatch = 'T';
        }
        else
        {
        	response->queuedata[i].pausedispatch = 'F';
        }
        response->queuedata[i].reserved[0] = '\0';
        response->queuedata[i].reserved[1] = '\0';
        response->queuedata[i].reserved[2] = '\0';
        response->queuedata[i].reserved[3] = '\0';
        response->queuedata[i].reserved[4] = '\0';
        response->queuedata[i].reserved[5] = '\0';
        response->queuedata[i].reserved[6] = '\0';
        i++; 
    }
    
    buildResponseIntoConnTrack(conntrack,
                               (char *)response,
                               responseLen,
                               conntrack->MessageMark1,
                               conntrack->MessageMark2,
                               RESPONSE_QD_DUMP_RESQUEUE_STATUS);
    conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK
    return true;
}

bool handleRMRequestDumpStatus(void **arg)
{
    ConnectionTrack conntrack   = (ConnectionTrack)(*arg);
    RPCResponseDumpStatusData response;

    RPCRequestDumpStatus request = SMBUFF_HEAD(RPCRequestDumpStatus,
    										   &(conntrack->MessageBuff));

    elog(DEBUG3, "resource manager dump type %u data to file %s",
    			 request->type,
				 request->dump_file);

    response.Result   = FUNC_RETURN_OK;
    response.Reserved = 0;
    switch (request->type)
    {
    case 1:
        dumpConnectionTracks(request->dump_file);
        break;
    case 2:
        dumpResourceQueueStatus(request->dump_file);
        break;
    case 3:
        dumpResourcePoolHosts(request->dump_file);
        break;
    default:
        Assert(false);
        break;
    }

    SelfMaintainBufferData responsedata;
    initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

    appendSMBVar(&responsedata, response);

    buildResponseIntoConnTrack(conntrack,
                               SMBUFF_CONTENT(&responsedata),
							   getSMBContentSize(&responsedata),
							   conntrack->MessageMark1,
                               conntrack->MessageMark2,
                               RESPONSE_QD_DUMP_STATUS);
    destroySelfMaintainBuffer(&responsedata);

    conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

    return true;
}

bool handleRMRequestDummy(void **arg)
{
    ConnectionTrack 	 conntrack = (ConnectionTrack)(*arg);
    RPCResponseDummyData response;
    response.Result = FUNC_RETURN_OK;
    response.Reserved = 0;
    buildResponseIntoConnTrack(conntrack,
                               (char *)&response,
                               sizeof(response),
                               conntrack->MessageMark1,
                               conntrack->MessageMark2,
                               RESPONSE_DUMMY);
    conntrack->ResponseSent = false;
    MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
    PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
    MEMORY_CONTEXT_SWITCH_BACK

    return true;
}

bool handleRMRequestQuotaControl(void **arg)
{
	ConnectionTrack conntrack = (ConnectionTrack)(*arg);
	RPCRequestQuotaControl request =
		SMBUFF_HEAD(RPCRequestQuotaControl, &(conntrack->MessageBuff));
	Assert(request->Phase >= 0 && request->Phase < QUOTA_PHASE_COUNT);
	bool olditem = PRESPOOL->pausePhase[request->Phase];
	PRESPOOL->pausePhase[request->Phase] = request->Pause;
	if ( olditem != PRESPOOL->pausePhase[request->Phase] )
	{
		elog(LOG, "resource manager resource quota life cycle pause setting %d "
				  "changes to %s",
				  request->Phase,
				  PRESPOOL->pausePhase[request->Phase]?"paused":"resumed");
	}

	RPCResponseQuotaControlData response;
	response.Result 	= FUNC_RETURN_OK;
	response.Reserved	= 0;

    buildResponseIntoConnTrack(conntrack,
                               (char *)&response,
                               sizeof(response),
                               conntrack->MessageMark1,
                               conntrack->MessageMark2,
							   RESPONSE_QD_QUOTA_CONTROL);
    conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}
