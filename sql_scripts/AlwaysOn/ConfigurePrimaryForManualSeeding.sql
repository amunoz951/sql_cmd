ALTER AVAILABILITY GROUP [$(availabilitygroupname)]
MODIFY REPLICA ON N'$(primarysqlserver)' WITH (SEEDING_MODE = MANUAL)
