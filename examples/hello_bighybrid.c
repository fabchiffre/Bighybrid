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


#include "common_bighybrid.h"
#include "mra_cv.h"
#include <xbt/RngStream.h>
#include <bighybrid.h>
#include <xbt/log.h> 


void BigHybrid_init ();

unsigned long int seed_map[] = {102, 33, 27, 86, 1, 392};
unsigned long int seed_reduce[] = {2, 2, 2 ,2 ,2 ,2};

RngStream stream_map = NULL;

static void read_mra_config_file (const char* file_name)
{
    char    property[256];
    FILE*   file;

    /* Set the default configuration. */
    config_mra.mra_chunk_size = 67108864;
    config_mra.amount_of_tasks_mra[MRA_REDUCE] = 1;
    config_mra.Fg=1;
    config_mra.mra_perc=100;

    /* Read the user configuration file. */

    file = fopen (file_name, "r");
    /* Read the user configuration file. */

    xbt_assert (file != NULL, "Error reading cofiguration file: %s", file_name);
    
    while ( fscanf (file, "%256s", property) != EOF )
    {
			if ( strcmp (property, "mra_chunk_size") == 0 )
			{
	    fscanf (file, "%lg", &config_mra.mra_chunk_size);
	    config_mra.mra_chunk_size *= 1024 * 1024; /* MB -> bytes */
			}
			else if ( strcmp (property, "grain_factor") == 0 )
			{
	    fscanf (file, "%d", &config_mra.Fg);
			}
			else if ( strcmp (property, "mra_intermed_perc") == 0 )
			{
	    fscanf (file, "%g", &config_mra.mra_perc);
			}
			else if ( strcmp (property, "mra_reduces") == 0 )
			{
	    fscanf (file, "%d", &config_mra.amount_of_tasks_mra[MRA_REDUCE]);
			}
			else
			{
	    printf ("Error: Property %s is not valid. (in %s)", property, file_name);
	    exit (1);
			}
	    
    }

    fclose (file);

}

static void read_mrsg_config_file (const char* file_name)
{
    char    property[256];
    FILE*   file;

    /* Set the default configuration. */
    config_mrsg.mrsg_chunk_size = 67108864;
    config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE] = 1;
    config_mrsg.mrsg_slots[MRSG_REDUCE] = 2;
    config_mrsg.mrsg_perc = 100;
    

    /* Read the user configuration file. */

    file = fopen (file_name, "r");

    xbt_assert (file != NULL, "Error reading cofiguration file: %s", file_name);

    while ( fscanf (file, "%256s", property) != EOF )
    {
			if ( strcmp (property, "mrsg_chunk_size") == 0 )
				{
	    		fscanf (file, "%lg", &config_mrsg.mrsg_chunk_size);
	    		config_mrsg.mrsg_chunk_size *= 1024 * 1024; /* MB -> bytes */
				}
			else if ( strcmp (property, "mrsg_reduces") == 0 )
				{
	    		fscanf (file, "%d", &config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
				}
			else if ( strcmp (property, "mrsg_intermed_perc") == 0 )
				{
	    		fscanf (file, "%g", &config_mrsg.mrsg_perc);
				}
			else
				{
	    			printf ("Error: Property %s is not valid. (in %s)", property, file_name);
	    			exit (1);
				}
    }

    fclose (file);
}


/**
 * User function that indicates the amount of bytes
 * that a map task will emit to a reduce task.
 *
 * @param  mid  The ID of the map task.
 * @param  rid  The ID of the reduce task.
 * @return The amount of data emitted (in bytes).
 */
int mra_map_mra_output_function (size_t mid, size_t rid)
{

		return ((config_mra.mra_chunk_size*config_mra.mra_perc/100)/config_mra.amount_of_tasks_mra[MRA_REDUCE]);
//     return 2*1024*1024;
}


int mrsg_map_output_function (size_t mid, size_t rid)
{

		return ((config_mrsg.mrsg_chunk_size*config_mrsg.mrsg_perc/100)/config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);

//    return 4*1024*1024;
}



/**
 * User function that indicates the MRA cost of a task.
 *
 * @param  mra_phase  The execution phase.
 * @param  tid    The ID of the task.
 * @param  mra_wid    The ID of the worker that received the task.
 * @return The task cost in FLOPs.
 */
double mra_task_mra_cost_function (enum mra_phase_e mra_phase, size_t tid, size_t mra_wid)
{
    switch (mra_phase)
    {
	case MRA_MAP:
	    return 3e+11;

	case MRA_REDUCE:
	    return (3e+11/config_mra.Fg);
    }
}

/**
 * User function that indicates the MRSG cost of a task.
 *
 * @param  mrsg_phase  The execution phase.
 * @param  tid    The ID of the task.
 * @param  mrsg_wid    The ID of the worker that received the task.
 * @return The task cost in FLOPs.
 */
 
double mrsg_task_cost_function (enum mrsg_phase_e mrsg_phase, size_t tid, size_t mrsg_wid)
{
    switch (mrsg_phase)
    {
	case MRSG_MAP:{
        // double d =  RngStream_RandU01(stream_map);
        // printf("Contention factor : %f\n", d);
        // return 3e+11 * (1+d);
	    return 3e+11;
    }
	case MRSG_REDUCE:
	    return 5e+11;
    }
}


int main (int argc, char* argv[])
{
    stream_map = RngStream_CreateStream(NULL);
    RngStream_SetSeed(stream_map, seed_map);

    /* MRA_user_init must be called before setting the user functions. */
    MRA_user_init ();
    /* MRSG_user_init must be called before setting the user functions. */
    MRSG_user_init ();
    /* Set the task cost MRA function. */
    MRA_set_task_mra_cost_f (mra_task_mra_cost_function);
    /* Set the task cost MRSG function. */
    MRSG_set_task_cost_f (mrsg_task_cost_function);
    /* Set the MRA_map output function. */
    MRA_set_map_mra_output_f (mra_map_mra_output_function);
    /* Set the MRSG_map output function. */
    MRSG_set_map_output_f (mrsg_map_output_function);

    /* Run the MRA simulation. */   
   // mra_input_main = {"mra-plat15-10M.xml", "d-mra-plat15-10M.xml", "mra15.conf"};
    //MRA_main ("mra-plat10-net_var.xml", "d-mra-plat10-net_var.xml", "mra10.conf");
    //MRA_main ("mra-plat32-10M.xml", "d-mra-plat32-10M.xml", "mra32.conf");
    //MRA_main ("mra-plat64-10M.xml", "d-mra-plat64-10M.xml", "mra64.conf");
    //MRA_main ("mra-plat128-1G.xml", "d-mra-plat128-1G.xml", "mra128.conf"); 
   // MRA_main ("mra-plat256-10M.xml", "d-mra-plat256-10M.xml", "mra256.conf");
  
    /* Run the MRSG simulation. */
   // mrsg_input_main = {"g5k.xml", "hello.deploy.xml", "hello_mrsg.conf"};
   
    /* Run the BigHybrid simulation. */  
    BIGHYBRID_main ("bighyb-plat5node.xml", "d-bighyb-plat5node.xml", "bighyb-plat5node.conf", "parse-boinc.txt");
      
   // BIGHYBRID_main ("bighyb-plat5hom_15het.xml","d-bighyb-plat5hom_15het.xml","bighyb-plat5-15.conf","parse-boinc.txt"); 



    return 0;
}

