IF EXISTS (SELECT * FROM sys.credentials
           WHERE name = '$(credential_name)')
BEGIN
  ALTER CREDENTIAL [$(credential_name)] WITH IDENTITY = '$(identity)', SECRET = '$(secret)';
  SELECT 'Credential [$(credential_name)] updated.'
END
ELSE
BEGIN
  CREATE CREDENTIAL [$(credential_name)] WITH IDENTITY = '$(identity)', SECRET = '$(secret)';
  SELECT 'Credential [$(credential_name)] created.'
END
