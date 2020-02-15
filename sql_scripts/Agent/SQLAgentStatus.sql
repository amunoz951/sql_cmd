IF EXISTS (SELECT 1 FROM master.dbo.sysprocesses WHERE program_name = N'SQLAgent - Generic Refresher')
BEGIN
  SELECT 1 AS 'SQLServerAgentRunning'
END
ELSE
BEGIN
  SELECT 0 AS 'SQLServerAgentRunning'
END
