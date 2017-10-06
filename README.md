# Ethnicity Detector
Given someone's first and last names, figure out what is the likeliest ethnicity of this person.

## Input 
We connect to the Microsoft SQL Server database on AWS and collect all new *customer ids* along with the associated *full names* (first name, middle name if any and last name). The criterion for which customer ids are new can be specified but it is those modified today by default. Th collected data is placed into a pandas data frame that has two columns: *cust_id* and *full_name*.