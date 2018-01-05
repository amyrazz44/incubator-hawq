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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG2RM_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG2RM_H

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

int sendIMAlive(int  *errorcode,
				char *errorbuf,
				int	  errorbufsize);
#endif /* RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG2RM_H */
