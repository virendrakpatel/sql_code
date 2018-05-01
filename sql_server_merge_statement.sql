

insert into ypc_bi_dw.dbo.D_SaturnCustomerDetail 
  (  /*Table and columns in which to insert the data*/
   CustomerKey, SalesPartner_CustomerCode, SalesRepCode, SuperviserCode, OfficeCode, SalesChannelCode, Active, StartDateKey, EndDateKey, DW_InsertDate, DW_InsertBy 
   ) 
   /*Select the rows/columns to insert that are output from this merge statement the rows to be inserted are the rows that have changed (UPDATE). */
  SELECT changes.CustomerKey, changes.SalesPartner_CustomerCode, changes.SalesRepCode, changes.SuperviserCode, changes.OfficeCode, changes.SalesChannelCode, \'Y\' Active, 
         CONVERT(char, GETDATE(),112) StartDateKey, 
         99990101  AS EndDateKey, 
         GETDATE() AS DW_InsertDate, 
         USER      AS DW_InsertBy 
  FROM 
  ( /* This is the beginning of the merge statement. The target must be defined, in this example it is our slowly changing dimension table  */ 
    MERGE ypc_bi_dw.dbo.D_SaturnCustomerDetail AS target 
    USING   
    ( 
        SELECT cus.CustomerKey, 
               act.Customer_ID__c as SalesPartner_Customercode,   
              ISNULL(rep.SalesRepCode,-999) as SalesRepCode,   
              ISNULL(rep.SuperviserCode,-999) as SuperviserCode,   
              ISNULL(rep.OfficeCode, -999) as OfficeCode, 
              ISNULL(rep.SFDC_SalesChannelCode, (select isnull(SalesChannelCode,-999) from ypc_bi_dw.dbo.D_SalesChannel where SalesChannelName =\'Premise\' and SalesPartnerKey = 4)) as SalesChannelCode, 
              \'Y\' Active, 
              19900101 StartDateKey, 
              99990101  AS EndDateKey, 
              GETDATE() AS DW_InsertDate, 
              USER      AS DW_InsertBy  
       FROM   ypc_bi_dw.dbo.STG_SFDC_Account act  
       INNER  JOIN ypc_bi_dw.dbo.D_Customer cus     ON ( act.ID = cus.SFDC_AccountId ) 
       LEFT   JOIN YPC_BI_DW.dbo.D_SalesRep rep     ON ( act.OwnerID = rep.SalesRepCode )
	    where act.Customer_ID__c is not null
    ) AS SOURCE  
    ON  /* We are matching on the CustomerKey in the target table and the source table. */
    (     target.CustomerKey = source.CustomerKey 
    ) 
    WHEN MATCHED and target.EndDateKey = 99990101
    AND	 target.StartDateKey = CONVERT(char, GETDATE(),112)  
    THEN  
        DELETE
    WHEN MATCHED and target.EndDateKey = 99990101
    AND	 target.StartDateKey <> CONVERT(char, GETDATE(),112)
    AND  (target.SalesRepCode <> source.SalesRepCode 
     OR   target.SuperviserCode <> source.SuperviserCode 
     OR   target.OfficeCode <> source.OfficeCode 
     OR   target.SalesChannelCode <> source.SalesChannelCode)  
    THEN  
        UPDATE SET target.EndDateKey = CONVERT(char, dateadd(d,-1,GETDATE()),112), 
                   target.Active =\'N\' 
    WHEN NOT MATCHED THEN  
    INSERT (CustomerKey, SalesPartner_CustomerCode, SalesRepCode, SuperviserCode, OfficeCode, SalesChannelCode, Active,StartDateKey,EndDateKey,DW_InsertDate,DW_InsertBy 
           )  
                  VALUES (SOURCE.CustomerKey, SOURCE.SalesPartner_CustomerCode, SOURCE.SalesRepCode, SOURCE.SuperviserCode, SOURCE.OfficeCode, SOURCE.SalesChannelCode, \'Y\' , 
                          19900101, 
                          99990101  , 
                          GETDATE() , 
                          USER      )  
          OUTPUT $action as Action, SOURCE.CustomerKey, SOURCE.SalesPartner_CustomerCode, SOURCE.SalesRepCode, SOURCE.SuperviserCode, SOURCE.OfficeCode, SOURCE.SalesChannelCode, SOURCE.Active, SOURCE.StartDateKey, SOURCE.EndDateKey, SOURCE.DW_InsertDate, SOURCE.DW_InsertBy 
  )  /* End of the merge statement */
  as changes  
  ( 
    Action, CustomerKey, SalesPartner_CustomerCode, SalesRepCode, SuperviserCode, OfficeCode, SalesChannelCode, Active, StartDateKey, EndDateKey, DW_InsertDate, DW_InsertBy 
  ) 
  where action in (\'UPDATE\',\'DELETE\');
