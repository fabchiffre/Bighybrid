static void init_job_mra (void)
{
    int     i;
    size_t  mra_wid;

    xbt_assert (config_mra.initialized, "init_mra_config has to be called before init_job_mra");

    job_mra.finished = 0;
    job_mra.mra_heartbeats = xbt_new (struct mra_heartbeat_s, config_mra.mra_number_of_workers);
    for (mra_wid = 0; mra_wid < config_mra.mra_number_of_workers; mra_wid++)
    {
	job_mra.mra_heartbeats[mra_wid].slots_av[MRA_MAP] = config_mra.mra_slots[MRA_MAP];
	job_mra.mra_heartbeats[mra_wid].slots_av[MRA_REDUCE] = config_mra.mra_slots[MRA_REDUCE];
    }

    /* Initialize map information. */
    job_mra.tasks_pending[MRA_MAP] = config_mra.amount_of_tasks_mra[MRA_MAP];
    job_mra.task_status[MRA_MAP] = xbt_new0 (int, config_mra.amount_of_tasks_mra[MRA_MAP]);
    job_mra.task_instances[MRA_MAP] = xbt_new0 (int, config_mra.amount_of_tasks_mra[MRA_MAP]);
    job_mra.task_list[MRA_MAP] = xbt_new0 (msg_task_t*, config_mra.amount_of_tasks_mra[MRA_MAP]);
    for (i = 0; i < config_mra.amount_of_tasks_mra[MRA_MAP]; i++)
	  job_mra.task_list[MRA_MAP][i] = xbt_new0 (msg_task_t, MAX_SPECULATIVE_COPIES);
	  
    /* Initialize Reduce tasks number. */
    if (Fg != 1) {
    		config_mra.amount_of_tasks_mra[MRA_REDUCE] = Fg * config_mra.mra_number_of_workers;
    		}
     
        
    job_mra.map_output = xbt_new (size_t*, config_mra.mra_number_of_workers);
    for (i = 0; i < config_mra.mra_number_of_workers; i++)
	  job_mra.map_output[i] = xbt_new0 (size_t, config_mra.amount_of_tasks_mra[MRA_REDUCE]);

    // Initialize reduce information. 

    job_mra.tasks_pending[MRA_REDUCE] = config_mra.amount_of_tasks_mra[MRA_REDUCE];
    job_mra.task_status[MRA_REDUCE] = xbt_new0 (int, config_mra.amount_of_tasks_mra[MRA_REDUCE]);
    job_mra.task_instances[MRA_REDUCE] = xbt_new0 (int, config_mra.amount_of_tasks_mra[MRA_REDUCE]);
    job_mra.task_list[MRA_REDUCE] = xbt_new0 (msg_task_t*, config_mra.amount_of_tasks_mra[MRA_REDUCE]);
    for (i = 0; i < config_mra.amount_of_tasks_mra[MRA_REDUCE]; i++)
	  job_mra.task_list[MRA_REDUCE][i] = xbt_new0 (msg_task_t, MAX_SPECULATIVE_COPIES);
	 // Configuracao dos Reduces Termina aqui */

}
