/* Copyright (c) 2014. BigHybrid Team. All rights reserved. */

/* This file is part of BigHybrid.

BigHybrid, MRSG and MRA++ are free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

BigHybrid, MRSG and MRA++ are distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with BigHybrid, MRSG and MRA++.  If not, see <http://www.gnu.org/licenses/>. */

#ifndef WORKERMRA_H
#define WORKERMRA_H

/* hadoop-config: mapred.max.tracker.failures */
#define MAXIMUM_WORKER_FAILURES 4

typedef struct mra_w_info_s {
	size_t  mra_wid;
}* w_mra_info_t ;

/**
 * @brief  Get the ID of a worker.
 * @param  worker  The worker node.
 * @return The worker's ID number.
 */
size_t get_mra_worker_id (msg_host_t worker);

#endif /* !WORKERMRA_H */
