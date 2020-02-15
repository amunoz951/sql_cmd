SET NOCOUNT ON

USE [$(databasename)]

DECLARE @user_principal_id int
SELECT @user_principal_id = principal_id FROM sys.database_principals
WHERE name like '$(user)' AND [sid] = $(sid)

SELECT * FROM sys.database_role_members drm
INNER JOIN sys.database_principals dp
	ON drm.role_principal_id = dp.principal_id
WHERE member_principal_id = @user_principal_id
