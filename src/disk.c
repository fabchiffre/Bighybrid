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

#include <msg/msg.h>
#include <xbt/log.h>
#include <xbt/dict.h>

XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

void write_task_on_disk(msg_task_t msg)
{
	double size = MSG_task_get_data_size(msg);

	xbt_dict_t  str_dict = MSG_host_get_mounted_storage_list (MSG_host_self ());
	msg_storage_t storage = xbt_dict_get_or_null(str_dict, "/");

	if(storage  != NULL && size != 0) 
	{
		// XBT_INFO("MRSG %s write %lf bytes", MSG_host_get_name(MSG_host_self ()), size);
	
		xbt_dict_free(&str_dict);
		char* file_name = xbt_strdup("/data.txt");
		msg_file_t file = MSG_file_open(file_name, NULL);
		MSG_file_write(file, size);
		MSG_file_close(file);
	} 
}