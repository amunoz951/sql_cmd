DECLARE @database_name NVARCHAR(512)
SET @database_name = '$(databasename)'

SELECT TOP 1
database_name,
ag.name as ag_name,
adc.database_name,
r.replica_server_name,
start_time,
completion_time,
current_state,
failure_state_desc,
number_of_attempts,
failure_condition_level,
transfer_rate_bytes_per_second,
transferred_size_bytes,
database_size_bytes,
start_time_utc,
end_time_utc,
estimate_time_complete_utc,
CASE WHEN current_state = 'COMPLETED' THEN 100 ELSE ROUND((CONVERT(float, estimate_time_complete_utc - start_time_utc) - CONVERT(float, estimate_time_complete_utc - GETUTCDATE()))/CONVERT(float, estimate_time_complete_utc - start_time_utc) * 100, 1) END AS [time_elapsed_percent_complete],
CASE WHEN current_state = 'COMPLETED' THEN 100 ELSE ROUND(CONVERT(float, transferred_size_bytes) / CONVERT(float, database_size_bytes) * 100, 1) END AS [transferred_size_percent_complete],
total_disk_io_wait_time_ms,
total_network_wait_time_ms,
is_compression_enabled
FROM sys.availability_groups ag
JOIN sys.availability_replicas r ON ag.group_id = r.group_id
JOIN sys.availability_databases_cluster adc on ag.group_id=adc.group_id
JOIN sys.dm_hadr_automatic_seeding AS dhas
ON dhas.ag_id = ag.group_id
LEFT JOIN sys.dm_hadr_physical_seeding_stats AS dhpss
ON adc.database_name = dhpss.local_database_name
WHERE database_name = @database_name
ORDER BY start_time DESC
