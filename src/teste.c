/**
 * @brief  Initialize the config structure in MRSG.
 */
static void init_mrsg_config (void)
{
    const char*    process_name = NULL;
    msg_host_t     host;
    msg_process_t  process;
    size_t         mrsg_wid;
    unsigned int   cursor;
    w_mrsg_info_t       wi;
    xbt_dynar_t    process_list;

    /* Initialize MRSG hosts information. */
    config_mrsg.mrsg_number_of_workers = 0;

    process_list = MSG_processes_as_dynar ();
    xbt_dynar_foreach (process_list, cursor, process)
    {
	process_name = MSG_process_get_name (process);
	if ( strcmp (process_name, "worker_mrsg") == 0 )
	    config_mrsg.mrsg_number_of_workers++;
    }
    config_mrsg.workers_mrsg = xbt_new (msg_host_t, config_mrsg.mrsg_number_of_workers);

    mrsg_wid = 0;
    config_mrsg.grid_cpu_power = 0.0;
    xbt_dynar_foreach (process_list, cursor, process)
    {
	process_name = MSG_process_get_name (process);
	host = MSG_process_get_host (process);
	if ( strcmp (process_name, "worker_mrsg") == 0 )
	{
	    config_mrsg.workers_mrsg[mrsg_wid] = host;
	    /* Set the worker ID as its data. */
	    wi = xbt_new (struct mrsg_w_info_s, 1);
	    wi->mrsg_wid = mrsg_wid;
	    MSG_host_set_data (host, (void*)wi);
	    /* Add the worker's cpu power to the grid total. */
	    config_mrsg.grid_cpu_power += MSG_get_host_speed (host);
	    mrsg_wid++;
	}
    }
    config_mrsg.grid_average_speed = config_mrsg.grid_cpu_power / config_mrsg.mrsg_number_of_workers;
    config_mrsg.mrsg_heartbeat_interval = mrsg_maxval (MRSG_HEARTBEAT_MIN_INTERVAL, config_mrsg.mrsg_number_of_workers / 100);
    config_mrsg.amount_of_tasks_mrsg[MRSG_MAP] = config_mrsg.mrsg_chunk_count;
    config_mrsg.initialized = 1;
}

/**
 * @brief  Initialize the job structure in MRSG.
 */
static void init_job_mrsg (void)
{
    int     i;
    size_t  mrsg_wid;

    xbt_assert (config_mrsg.initialized, "init_config has to be called before init_job");

    job_mrsg.finished = 0;
    job_mrsg.mrsg_heartbeats = xbt_new (struct mrsg_heartbeat_s, config_mrsg.mrsg_number_of_workers);
    for (mrsg_wid = 0; mrsg_wid < config_mrsg.mrsg_number_of_workers; mrsg_wid++)
    {
	job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[MRSG_MAP] = config_mrsg.mrsg_slots[MRSG_MAP];
	job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[MRSG_REDUCE] = config_mrsg.mrsg_slots[MRSG_REDUCE];
    }
    /* Initialize map information. */
    job_mrsg.tasks_pending[MRSG_MAP] = config_mrsg.amount_of_tasks_mrsg[MRSG_MAP];
    job_mrsg.task_status[MRSG_MAP] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]);
    job_mrsg.task_instances[MRSG_MAP] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]);
    job_mrsg.task_list[MRSG_MAP] = xbt_new0 (msg_task_t*, config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]);
    for (i = 0; i < config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]; i++)
	job_mrsg.task_list[MRSG_MAP][i] = xbt_new0 (msg_task_t, MAX_SPECULATIVE_COPIES);

    job_mrsg.map_output = xbt_new (size_t*, config_mrsg.mrsg_number_of_workers);
    for (i = 0; i < config_mrsg.mrsg_number_of_workers; i++)
	job_mrsg.map_output[i] = xbt_new0 (size_t, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);

    /* Initialize reduce information. */
    job_mrsg.tasks_pending[MRSG_REDUCE] = config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE];
    job_mrsg.task_status[MRSG_REDUCE] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    job_mrsg.task_instances[MRSG_REDUCE] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    job_mrsg.task_list[MRSG_REDUCE] = xbt_new0 (msg_task_t*, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    for (i = 0; i < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; i++)
	job_mrsg.task_list[MRSG_REDUCE][i] = xbt_new0 (msg_task_t, MAX_SPECULATIVE_COPIES);
}
/**
 * @brief  Free allocated memory for global variables MRSG.
 */
static void free_mrsg_global_mem (void)
{
    size_t  imrsg;

    for (imrsg = 0; imrsg < config_mrsg.mrsg_chunk_count; imrsg++)
	xbt_free_ref (&chunk_owner_mrsg[imrsg]);
    xbt_free_ref (&chunk_owner_mrsg);

    xbt_free_ref (&config_mrsg.workers_mrsg);
    xbt_free_ref (&job_mrsg.task_status[MRSG_MAP]);
    xbt_free_ref (&job_mrsg.task_instances[MRSG_MAP]);
    xbt_free_ref (&job_mrsg.task_status[MRSG_REDUCE]);
    xbt_free_ref (&job_mrsg.task_instances[MRSG_REDUCE]);
    xbt_free_ref (&job_mrsg.mrsg_heartbeats);
    for (imrsg = 0; imrsg < config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]; imrsg++)
	xbt_free_ref (&job_mrsg.task_list[MRSG_MAP][imrsg]);
    xbt_free_ref (&job_mrsg.task_list[MRSG_MAP]);
    for (imrsg = 0; imrsg < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; imrsg++)
	xbt_free_ref (&job_mrsg.task_list[MRSG_REDUCE][imrsg]);
    xbt_free_ref (&job_mrsg.task_list[MRSG_REDUCE]);
}
