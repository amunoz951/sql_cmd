ALTER AVAILABILITY GROUP [$(availabilitygroupname)]
MODIFY REPLICA ON N'$(secondaryreplica)' WITH (SEEDING_MODE = $(seedingmode))
