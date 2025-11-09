// Power Query M - Extract Work Orders from Salesforce (Last 12 Months)
let
    Source = Salesforce.Data("https://yourinstance.salesforce.com", [LoginPrompt="Automatic"]),
    WorkOrder = Source{[Name="WorkOrder"]}[Data],
    #"Removed Other Columns" = Table.SelectColumns(WorkOrder,{"Id", "WorkOrderNumber", "Subject", "Status", "Priority", "CreatedDate", "CreatedBy", "Hours_Worked__c", "Account", "Contact"}),
    #"Filtered Rows" = Table.SelectRows(#"Removed Other Columns", each Date.IsInPreviousNDays([CreatedDate], 365)),
    #"Changed Type" = Table.TransformColumnTypes(#"Filtered Rows",{{"CreatedDate", type datetime}, {"Hours_Worked__c", type number}}),
    #"Sorted Rows" = Table.Sort(#"Changed Type",{{"CreatedDate", Order.Descending}})
in
    #"Sorted Rows"
