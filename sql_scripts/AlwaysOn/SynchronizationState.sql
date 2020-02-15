SET NOCOUNT ON

SELECT dbrs.synchronization_state_desc, dbrs.synchronization_health_desc
FROM master.sys.availability_groups AS AG
LEFT OUTER JOIN master.sys.dm_hadr_availability_group_states as agstates
    ON AG.group_id = agstates.group_id
INNER JOIN master.sys.availability_replicas AS AR
    ON AG.group_id = AR.group_id
INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates
    ON AR.replica_id = arstates.replica_id AND arstates.is_local = 1 AND arstates.role = 2 -- arstates.role: 1=Primary, 2=Secondary
INNER JOIN master.sys.dm_hadr_database_replica_cluster_states AS dbcs
    ON arstates.replica_id = dbcs.replica_id AND dbcs.database_name LIKE '$(databasename)' AND dbcs.is_database_joined = 1
LEFT OUTER JOIN master.sys.dm_hadr_database_replica_states AS dbrs
    ON dbcs.replica_id = dbrs.replica_id AND dbcs.group_database_id = dbrs.group_database_id AND dbrs.synchronization_state = 2 AND ISNULL(dbrs.is_suspended, 0) = 0
