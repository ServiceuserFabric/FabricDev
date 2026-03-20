CREATE PROCEDURE [dbo].[Update_Puzzel_Agents]
AS
BEGIN
   /* WITH StageDate AS
    (
       SELECT a.[agent_id]
      ,a.[customer_key]
      ,a.[user_name]
      ,a.[user_num]
      ,a.[full_name]
      ,a.[usergroup_id]
      ,a.[usergroup_name]
      ,a.[email]
      ,a.[mobile]
      ,a.[dte_updated]
      ,a.[chat_role]
      ,a.[chat_master_user_id]
      ,a.[unblockable_role]
      ,a.[unblockable_group]
      ,a.[deleted]
      ,a.[puzzel_id]
      ,m.[user_name] as [master_user_name]
      ,m.[full_name] as [master_full_name]
      ,m.[email] as [master_email]
  FROM [Puzzel_Altibox].[dbo].[agents] AS a
  LEFT JOIN [Puzzel_Altibox].[dbo].[agents] AS m ON a.[chat_master_user_id] = m.[agent_id]
    )
    */
    MERGE puz.[agents] AS t 
    USING (
       SELECT a.[agent_id]
      ,a.[customer_key]
      ,a.[user_name]
      ,a.[user_num]
      ,a.[full_name]
      ,a.[usergroup_id]
      ,a.[usergroup_name]
      ,a.[email]
      ,a.[mobile]
      ,a.[dte_updated]
      ,a.[chat_role]
      ,a.[chat_master_user_id]
      ,a.[unblockable_role]
      ,a.[unblockable_group]
      ,a.[deleted]
      ,a.[puzzel_id]
      ,m.[user_name] as [master_user_name]
      ,m.[full_name] as [master_full_name]
      ,m.[email] as [master_email]
  FROM [Puzzel_Altibox].[dbo].[agents] AS a
  LEFT JOIN [Puzzel_Altibox].[dbo].[agents] AS m ON a.[chat_master_user_id] = m.[agent_id]
    ) 
    AS s
    ON [t].[agent_id] = [s].[agent_id]
    WHEN MATCHED AND
    ([t].[customer_key] <> [s].[customer_key] AND [s].[customer_key] IS NOT NULL) OR
    ([t].[user_name] <> [s].[user_name] AND [s].[user_name] IS NOT NULL) OR
    ([t].[user_num] <> [s].[user_num] AND [s].[user_num] IS NOT NULL) OR
    ([t].[full_name] <> [s].[full_name] AND [s].[full_name] IS NOT NULL) OR
    ([t].[usergroup_id] <> [s].[usergroup_id] AND [s].[usergroup_id] IS NOT NULL) OR
    ([t].[usergroup_name] <> [s].[usergroup_name] AND [s].[usergroup_name] IS NOT NULL) OR
    ([t].[email] <> [s].[email] AND [s].[email] IS NOT NULL) OR
    ([t].[mobile] <> [s].[mobile] AND [s].[mobile] IS NOT NULL) OR
    ([t].[dte_updated] <> [s].[dte_updated] AND [s].[dte_updated] IS NOT NULL) OR
    ([t].[chat_role] <> [s].[chat_role] AND [s].[chat_role] IS NOT NULL) OR
    ([t].[chat_master_user_id] <> [s].[chat_master_user_id] AND [s].[chat_master_user_id] IS NOT NULL) OR
    ([t].[unblockable_role] <> [s].[unblockable_role] AND [s].[unblockable_role] IS NOT NULL) OR
    ([t].[unblockable_group] <> [s].[unblockable_group] AND [s].[unblockable_group] IS NOT NULL) OR
    ([t].[deleted] <> [s].[deleted] AND [s].[deleted] IS NOT NULL) OR
    ([t].[puzzel_id] <> [s].[puzzel_id] AND [s].[puzzel_id] IS NOT NULL) OR
    ([t].[master_user_name] <> [s].[master_user_name] AND [s].[master_user_name] IS NOT NULL) OR
    ([t].[master_full_name] <> [s].[master_full_name] AND [s].[master_full_name] IS NOT NULL) OR
    ([t].[master_email] <> [s].[master_email] AND [s].[master_email] IS NOT NULL)
    THEN UPDATE SET
    [customer_key] = s.customer_key,
    [user_name] = s.user_name,
    [user_num] = s.user_num,
    [full_name] = s.full_name,
    [usergroup_id] = s.usergroup_id,
    [usergroup_name] = s.usergroup_name,
    [email] = s.email,
    [mobile] = s.mobile,
    [dte_updated] = s.dte_updated,
    [chat_role] = s.chat_role,
    [chat_master_user_id] = s.chat_master_user_id,
    [unblockable_role] = s.unblockable_role,
    [unblockable_group] = s.unblockable_group,
    [deleted] = s.deleted,
    [puzzel_id] = s.puzzel_id,
    [master_user_name] = s.master_user_name,
    [master_full_name] = s.master_full_name,
    [master_email] = s.master_email
    WHEN NOT MATCHED THEN INSERT (
    [agent_id],
    [customer_key],
    [user_name],
    [user_num],
    [full_name],
    [usergroup_id],
    [usergroup_name],
    [email],
    [mobile],
    [dte_updated],
    [chat_role],
    [chat_master_user_id],
    [unblockable_role],
    [unblockable_group],
    [deleted],
    [puzzel_id],
    [master_user_name],
    [master_full_name],
    [master_email]
    )
    VALUES (
    s.[agent_id],
    s.[customer_key],
    s.[user_name],
    s.[user_num],
    s.[full_name],
    s.[usergroup_id],
    s.[usergroup_name],
    s.[email],
    s.[mobile],
    s.[dte_updated],
    s.[chat_role],
    s.[chat_master_user_id],
    s.[unblockable_role],
    s.[unblockable_group],
    s.[deleted],
    s.[puzzel_id],
    s.[master_user_name],
    s.[master_full_name],
    s.[master_email]);

END