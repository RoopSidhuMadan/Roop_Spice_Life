# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 13:26:26 2021
ybl_aeps with envs

@author: simranjeet.kaur
"""
import numpy as np
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from datetime import date as d
import os
from google.oauth2 import service_account
from google.cloud import storage
from datetime import timedelta

dates = d.today()
times = datetime.now()
f = open('/home/sdlreco/crons/ybl_aeps/stat/stat-'+str(dates)+'.txt', 'a+')
f.close()

# key_path='/home/sdlreco/crons/ybl_aeps/spicemoney-dwh-aman-key.json'

def main():

    date = d.today()-timedelta(1)
    current_date5 = date.strftime('%d-%m-%Y')

    date = date.today()-timedelta(1)
    current_date2 = date.strftime('%d-%m-%Y')

    date = date.today()-timedelta(2)
    current_date3 = date.strftime('%d-%m-%Y')

    date = date.today()
    current_date4 = date.strftime('%d-%m-%Y')

    date = date.today()
    current_date6 = date.strftime('%Y%m%d')

    current_date = date.today()-timedelta(1)

    date = date.today()
    current_year = date.strftime('%Y')

    date = date.today()
    current_month = date.strftime('%m')

    date = date.today()
    current_day = date.strftime('%d')

    # credentials = service_account.Credentials.from_service_account_file(key_path)
    project_id = 'spicemoney-dwh'
    client = bigquery.Client(
                             project=project_id, location='asia-south1')
    
    fa=open('/home/sdlreco/crons/ybl_aeps/error/missing-'+str(date)+'.txt', 'w')
    fa.close()

    file_path = [str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/YBLAEPSTransaction File/1003693247_'+str(current_date3)+'_C1_F.csv', str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/YBLAEPSTransaction File/1003693247_'+str(current_date2)+'_C2_F.csv', str(current_year)+'/'+str(current_month)+'/'+str(current_day) +
                 '/YBLAEPSBank Settlement/YBL_BS_'+str(current_date6)+'_001481400000290.CSV', str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/YBLAEPSTransaction File/1003693247_'+str(current_date3)+'_C1_NF.csv', str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/YBLAEPSTransaction File/1003693247_'+str(current_date2)+'_C2_NF.csv']

    def status(projectname,  bucket_name, filename, stat):
        client = storage.Client(projectname)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(filename)
        print("{} --- {}".format(filename, blob.exists()))
        stat.append(blob.exists())
        if(not blob.exists()):
            with open('/home/sdlreco/crons/ybl_aeps/error/missing-'+str(date)+'.txt', 'a+') as f:
                f.write(filename)
                f.write('\n')
    bucket_name = 'sm-prod-rpa'
    stat = []
    for path in file_path:
        status(project_id,  bucket_name, path, stat)
    if False in stat:
        print('Files missing : Logged at -- /home/sdlreco/crons/ybl_aeps/error/missing-'+str(date)+'.txt')
    else:
        print('All files found, Processing ...')
        # bank statement
        #log file 1
        # credentials = service_account.Credentials.from_service_account_file(key_path)
        project_id = 'spicemoney-dwh'

        client = bigquery.Client( project=project_id, location='asia-south1')


        #log file 1


        schema = [{'name':'transaction_id','type':'INTEGER'},
                {'name':'transaction_date','type':'TIMESTAMP'},
                {'name':'transaction_amount','type':'INTEGER'},
                {'name':'iin','type':'INTEGER'},
                {'name':'stan','type':'INTEGER'},
                {'name':'rrn','type':'INTEGER'},
                {'name':'transaction_status','type':'INTEGER'},
                {'name':'response_code','type':'STRING'},
                {'name':'isvr','type':'INTEGER'},
                {'name':'retailer_transaction_id','type':'INTEGER'},
                {'name':'user_id','type':'INTEGER'},
                {'name':'retailer_id','type':'INTEGER'},
                {'name':'transaction_type_code','type':'INTEGER'}]

        header_list = ['transaction_id','transaction_date','transaction_amount','iin','stan','rrn','transaction_status','response_code','isvr','retailer_transaction_id','user_id','retailer_id','transaction_type_code']          

        df = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/YBLAEPSTransaction File/1003693247_'+str(current_date3)+'_C1_F.csv' ,delimiter = "~",names=header_list, dayfirst= True,skiprows=1 ,parse_dates = (['transaction_date']))



        df.to_gbq(destination_table='sm_recon.ts_aeps_ybl_logs', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema)
        df.to_gbq(destination_table='prod_sm_recon.prod_aeps_ybl_logs', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema)


        #log file 2

        # credentials = service_account.Credentials.from_service_account_file(key_path)
        project_id = 'spicemoney-dwh'

        client = bigquery.Client( project=project_id, location='asia-south1')


        schema = [{'name':'transaction_id','type':'INTEGER'},
                {'name':'transaction_date','type':'TIMESTAMP'},
                {'name':'transaction_amount','type':'INTEGER'},
                {'name':'iin','type':'INTEGER'},
                {'name':'stan','type':'INTEGER'},
                {'name':'rrn','type':'INTEGER'},
                {'name':'transaction_status','type':'INTEGER'},
                {'name':'response_code','type':'STRING'},
                {'name':'isvr','type':'INTEGER'},
                {'name':'retailer_transaction_id','type':'INTEGER'},
                {'name':'user_id','type':'INTEGER'},
                {'name':'retailer_id','type':'INTEGER'},
                {'name':'transaction_type_code','type':'INTEGER'}]

        header_list = ['transaction_id','transaction_date','transaction_amount','iin','stan','rrn','transaction_status','response_code','isvr','retailer_transaction_id','user_id','retailer_id','transaction_type_code']          

        df = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/YBLAEPSTransaction File/1003693247_'+str(current_date2)+'_C2_F.csv' ,delimiter = "~",names=header_list,skiprows=1, dayfirst=True , parse_dates = (['transaction_date']))



        df.to_gbq(destination_table='sm_recon.ts_aeps_ybl_logs', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema)
        df.to_gbq(destination_table='prod_sm_recon.prod_aeps_ybl_logs', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema)


        # log file 3

        # credentials = service_account.Credentials.from_service_account_file(key_path)
        project_id = 'spicemoney-dwh'

        client = bigquery.Client( project=project_id, location='asia-south1')


        schema = [{'name':'transaction_id','type':'INTEGER'},
                {'name':'transaction_date','type':'TIMESTAMP'},
                {'name':'transaction_amount','type':'INTEGER'},
                {'name':'iin','type':'INTEGER'},
                {'name':'stan','type':'INTEGER'},
                {'name':'rrn','type':'INTEGER'},
                {'name':'transaction_status','type':'INTEGER'},
                {'name':'response_code','type':'STRING'},
                {'name':'isvr','type':'INTEGER'},
                {'name':'retailer_transaction_id','type':'INTEGER'},
                {'name':'user_id','type':'INTEGER'},
                {'name':'retailer_id','type':'INTEGER'},
                {'name':'transaction_type_code','type':'INTEGER'}]

        header_list = ['transaction_id','transaction_date','transaction_amount','iin','stan','rrn','transaction_status','response_code','isvr','retailer_transaction_id','user_id','retailer_id','transaction_type_code']          

        df = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/YBLAEPSTransaction File/1003693247_'+str(current_date3)+'_C1_NF.csv' ,delimiter = "~",names=header_list,skiprows=1, dayfirst=True , parse_dates = (['transaction_date']))



        df.to_gbq(destination_table='sm_recon.ts_aeps_ybl_logs', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema)
        df.to_gbq(destination_table='prod_sm_recon.prod_aeps_ybl_logs', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema)


        # log file 4

        # credentials = service_account.Credentials.from_service_account_file(key_path)
        project_id = 'spicemoney-dwh'

        client = bigquery.Client( project=project_id, location='asia-south1')


        schema = [{'name':'transaction_id','type':'INTEGER'},
                {'name':'transaction_date','type':'TIMESTAMP'},
                {'name':'transaction_amount','type':'INTEGER'},
                {'name':'iin','type':'INTEGER'},
                {'name':'stan','type':'INTEGER'},
                {'name':'rrn','type':'INTEGER'},
                {'name':'transaction_status','type':'INTEGER'},
                {'name':'response_code','type':'STRING'},
                {'name':'isvr','type':'INTEGER'},
                {'name':'retailer_transaction_id','type':'INTEGER'},
                {'name':'user_id','type':'INTEGER'},
                {'name':'retailer_id','type':'INTEGER'},
                {'name':'transaction_type_code','type':'INTEGER'}]

        header_list = ['transaction_id','transaction_date','transaction_amount','iin','stan','rrn','transaction_status','response_code','isvr','retailer_transaction_id','user_id','retailer_id','transaction_type_code']          

        df = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/YBLAEPSTransaction File/1003693247_'+str(current_date2)+'_C2_NF.csv' ,delimiter = "~",names=header_list, skiprows=1, dayfirst=True , parse_dates = (['transaction_date']))



        df.to_gbq(destination_table='sm_recon.ts_aeps_ybl_logs', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema)
        df.to_gbq(destination_table='prod_sm_recon.prod_aeps_ybl_logs', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema)


        #bank statement


        # credentials = service_account.Credentials.from_service_account_file(key_path)
        project_id = 'spicemoney-dwh'

        client = bigquery.Client( project=project_id, location='asia-south1')

        header_list = ['row_number','transaction_date','cod_account_number','narration','value_date','cheque_number','drcr_flag','amount','date_post','running_balance','urn','bank_reference_number','cwd_rrn']

        schema = [{'name':'row_number','type':'STRING'},
                {'name':'transaction_date','type':'TIMESTAMP'},
                {'name':'cod_account_number','type':'INTEGER'},
                {'name':'narration','type':'STRING'},
                {'name':'value_date','type':'DATE'},
                {'name':'cheque_number','type':'STRING'},
                {'name':'drcr_flag','type':'STRING'},
                {'name':'amount','type':'FLOAT'},
                {'name':'date_post','type':'DATE'},
                {'name':'running_balance','type':'FLOAT'},
                {'name':'urn','type':'STRING'},
                {'name':'bank_reference_number','type':'STRING'},
                {'name':'cwd_rrn','type':'STRING'}]
                

        df = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/YBLAEPSBank Settlement/YBL_BS_'+str(current_date6)+'_001481400000290.CSV', skiprows=1, names=header_list, parse_dates = (['value_date', 'date_post', 'transaction_date']), low_memory=False )

        df.drop(('row_number'), axis=1, inplace=True)

        df['value_date'] = df['value_date'].dt.date
        df['date_post'] = df['date_post'].dt.date

        rrn1 = df['narration'].str.split(":").str[1]
        rrn2 = df['narration'].str.split("/").str[2]

        conditions = [(df['narration'].str.contains("AEPS ACQ CWD", na=False)),
                    (df['narration'].str.contains("Cash Withdrawal", na=False))
                    
        ]

        values = [rrn1,rrn2]
        df['cwd_rrn'] = np.select(conditions,values)

        df.to_gbq(destination_table='sm_recon.ts_aeps_ybl_bank_statement', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema)
        df.to_gbq(destination_table='prod_sm_recon.prod_aeps_ybl_bank_statement', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema)


        # bank summary

        sql_query = """
select 
    a.*, new_particular , amount 
	from
        (
		    select 
			        date(date_post) as date,
					cast(sum(case when narration like '%AEPS/Cash Withdrawal%' OR narration like 'AEPS ACQ CWD%'
          then amount else 0 end) as int64) as cash_withdrawal,
					cast(sum(case when  (narration not like '%AEPS BC COM:SPICE DIGITAL:YAU DT%' and  narration like '%AEPS BC COM%' OR narration like 'AEPS SPICE')  
          then amount else 0 end) as int64) as commission,
          
					cast(sum(case when narration like '%AEPS BC COM:SPICE DIGITAL:YAU DT%' then amount else 0 end) as int64) as YBLN_CW_Income,
					cast(sum(case when narration like '%AEPS MS COM:SPICE DIGITAL:YAU DT%' then amount else 0 end) as int64) as YBLN_MS_Income,
					cast(sum(case when narration like '%AEPS ACQ CR ADJ%'  then amount else 0 end) as int64) as credit_adjustment,
					cast(sum(case when narration like '%AEPS YAU CR ADJ%'  then amount else 0 end) as int64) as credit_adjustment_ybln,
					
					cast(sum(case when narration like '%Sweep/%' then amount else 0 end) as int64) as others,
					cast(sum(case when narration like '%Fraud%' then amount else 0 end) as int64) as fraud_amount,
			
					cast(sum(case when narration like '%AEPS/Cash Withdrawal%' then amount else 0 end) as int64)+
					cast(sum(case when narration like '%AEPS BC COM%' OR narration like 'AEPS SPICE' then amount else 0 end) as int64)+
					cast(sum(case when narration like '%AEPS ACQ CR ADJ%' or narration like '%AEPS YAU CR ADJ%' then amount else 0 end) as int64)+
					cast(sum(case when narration like '%Sweep/%' then amount else 0 end) as int64) as total
        
			from `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
			where date(date_post) = @date
			group by date
		
		
		)a
        
        left join 
        
        (select date(date_post) as date2, amount,
        
        (case when narration not like '%AEPS/Cash Withdrawal%'and narration not like 'AEPS ACQ CWD%' and narration not like '%Fraud%' and narration not like '%AEPS BC COM%' and narration not like 'AEPS SPICE' and
        (narration not like  '%AEPS ACQ CR ADJ%' or narration like '%AEPS YAU CR ADJ%') and narration not like '%Sweep/%' and narration not  like 'OPENING BALANCE' and 
        narration not like 'CLOSING BALANCE' then narration end) as new_particular
        
        from `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
        where date(date_post) = @date and 
        narration not like '%AEPS/Cash Withdrawal%' and narration not like 'AEPS ACQ CWD%' and narration not like '%Fraud%' and narration not like '%AEPS BC COM%' and narration not like '%AEPS SPICE%' and    narration not like '%AEPS MS COM%'  and 
        (narration not like  '%AEPS ACQ CR ADJ%' and narration not like '%AEPS YAU CR ADJ%') and narration not like '%Sweep/%' and narration not  like '%OPENING BALANCE%' and 
        narration not like '%CLOSING BALANCE%')b
        
        on a.date = b.date2

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_bank_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        # transaction table

        sql_query = """
        select date(a.log_date_time) as transaction_date, (c.Transaction_ID) as trans_id, cast(c.RRN as string) as ybl_rrn,(a.jdt_stan) as sdl_rrn,cast(c.TRANSaction_AMOUNT/100 as int64) as ybl_amount,
        (a.amount) as sdl_amount,(c.RESPonse_CODE) as ybl_response_code,(a.rc) as sdl_response_code,(c.USER_ID) as user_id,(c.RETAILER_ID) as retailer_id, (c.STAN) as stan,
        (b.aggregator) as aggregator
        from `spicemoney-dwh.prod_dwh.aeps_trans_res_v2` a
        join prod_dwh.aeps_trans_req b on a.request_id = b.request_id inner join `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs` c on a.jdt_stan = cast(c.RRN as string) 
        WHERE
        DATE(a.log_date_time) between DATE (@date) AND date (@date) AND
        date(b.log_date_time) between date (@date) AND date (@date) AND 
        b.aggregator = 'YBL'
        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_transaction', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_transaction', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        # summary 

        sql_query = """
        select * 
        from 
        (
        select t1.transaction_date,t8.sdl_transaction_count,t1.total_sdl_payout,t1.sdl_share,t1.sdl_share_with_gst,
        t2.aeps_alliance_commission,t2.aeps_sub_distributor_commission,
        t2.aeps_transaction_commission,t2.aeps_distributor_transaction_commission,
        t2.total_commission_credited,t2.ftr_agent_wallet_credit_amount,t8.amount_detailed_logs,
        (t2.ftr_agent_wallet_credit_amount - t8.amount_detailed_logs)as difference_ftr_vs_sdl_logs,
        t7.npci_amount,t3.amount_bank_statement,
        (t3.amount_bank_statement - t7.npci_amount) as difference_bank_logs_vs_amount_settled_in_bank,
        (t3.amount_bank_statement - t8.amount_detailed_logs) as excess_settled_by_bank_as_per_bank_statement,
        (t7.npci_amount - t8.amount_detailed_logs) as excess_settled_by_bank_as_per_npci_logs,
        t3.amount_per_bank_statement_credit_adjustment_processed,
        ((t3.amount_bank_statement - t8.amount_detailed_logs) + t3.amount_per_bank_statement_credit_adjustment_processed) as difference_sdl_vs_bank_after_adjustment,
        t1.total_sdl_payout as total_amount_eligible_sdl_payout,t1.sdl_share as total_sdl_share,t1.tds_deduction, t1.sdl_share_with_gst as total_sdl_share_with_gst,
        (t1.sdl_share - t1.tds_deduction) as settlement_charges_credited_by_bank,((t1.sdl_share - t1.tds_deduction)*10/100) as ten_percent_hold_by_bank,
        ((t1.sdl_share - t1.tds_deduction) - ((t1.sdl_share - t1.tds_deduction)*10/100)) as net_income_credited_as_on_date,
        t3.amount_credited_as_per_bank_statement, (t1.sdl_share_with_gst - t1.sdl_share) as gst_on_settlement_amount,
        ((t1.sdl_share - t1.tds_deduction) + (t1.sdl_share_with_gst - t1.sdl_share + t7.sdl_income_gst ) - t3.amount_credited_as_per_bank_statement) as difference_sdl_vs_bank_income_plus_gst,
        t7.mini_statement_count_as_per_bank , 
        t7.tds_on_mini_statement_income,t7.mini_statement_income ,t7. mini_statement_ten_percent_hold_by_bank,
        (t7.mini_statement_income - t7. mini_statement_ten_percent_hold_by_bank) as net_credit_by_bank_on_recon_date,
        t7.gst_on_mini_statement_income ,t7.sdl_income_gst,t5.mini_statement_count_spice,
        (t7.mini_statement_count_as_per_bank - t5.mini_statement_count_spice) as difference_spice_vs_bank_count, t1.aggregator 
        
        from
        (select date(transaction_date) as transaction_date , aggregator as aggregator,
        count(ybl_rrn) as ybl_transaction_count,
        cast(sum(case when ybl_amount*0.005>15 then 15 else ybl_amount*0.005 end) as int64) as total_sdl_payout,
        cast(sum(case when ybl_amount*0.005>15 then 15 else ybl_amount*0.005 end)*0.93 as int64) as sdl_share,
        cast(sum(case when ybl_amount*0.005>15 then 15 else ybl_amount*0.005 end)*0.93*1.18 as int64) as sdl_share_with_gst,
        CAST(SUM(sdl_amount) AS int64) as  amount_detailed_logs1, 
        cast(sum(ybl_amount) as int64) as npci_amount1,
        CAST(sum(case when ybl_amount*0.005>15 then 15 else ybl_amount*0.005 end)*0.93*0.05 AS int64) as tds_deduction
        
        from `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_transaction`
        where sdl_response_code = '00' AND 
        ybl_response_code = '00' AND
        date(transaction_date) = date (@date)
        group by transaction_date , aggregator) as t1
        
        left outer join
        (select date(a.log_date_time) as date2,
        cast(sum(case when b.comments = 'AEPS RBL Alliance Transaction Commission' then b.amount_transferred else 0 end) as int64) as aeps_alliance_commission,
        cast(sum(case when b.comments = 'AEPS RBL Sub Distr Transaction Commission' then b.amount_transferred else 0 end) as int64) as aeps_sub_distributor_commission,
        cast(sum(case when b.comments = 'AEPS RBL Transaction Commission' then b.amount_transferred else 0 end) as int64) as aeps_transaction_commission,
        cast(sum(case when b.comments = 'AEPS RBL Distr Transaction Commission' then b.amount_transferred else 0 end) as int64)+
        cast(sum(case when b.comments = 'AEPS-CW Dist Commission' then b.amount_transferred else 0 end) as int64) as aeps_distributor_transaction_commission,
        cast(sum(case when b.comments = 'AEPS RBL Alliance Transaction Commission' then b.amount_transferred else 0 end) as int64)+ 
        cast(sum(case when b.comments = 'AEPS RBL Transaction Commission' then b.amount_transferred else 0 end) as int64)+
        cast(sum(case when b.comments = 'AEPS RBL Distr Transaction Commission' then b.amount_transferred else 0 end) as int64)+
        cast(sum(case when b.comments = 'AEPS-CW Dist Commission' then b.amount_transferred else 0 end) as int64)+
        cast(sum(case when b.comments = 'AEPS RBL Sub Distr Transaction Commission' then b.amount_transferred else 0 end) as int64) as total_commission_credited,
        cast(sum(case when b.comments = 'AEPS RBL- Cash Withdrawal'or b.comments like '%AEPS-CASH WITHDRAWAL%' then b.amount_transferred else 0 end) as int64) as ftr_agent_wallet_credit_amount
        
        from `spicemoney-dwh.prod_dwh.aeps_trans_res_v2` a 
        join prod_dwh.cme_wallet b on a.request_id = b.unique_identification_no  join `spicemoney-dwh.prod_dwh.aeps_trans_req` c on a.request_id = c.request_id
        where date(a.log_date_time) = date (@date) AND 
        a.rc = '00' AND
        Date(b.transfer_date) = date (@date) AND
        c.aggregator = 'YBL' AND
        Date(c.log_date_time) = date (@date)
        group by date2) as t2 on t1.transaction_date = t2.date2
        
        left outer join 
        (select date(date_post) as date3,
        cast(sum(case when NARRATION like '%AEPS/Cash Withdrawal%' or  NARRATION like '%AEPS ACQ CWD%' then AMOUNT else 0 end) as int64) as amount_bank_statement,
        cast(sum(case when (NARRATION not like '%AEPS BC COM:SPICE DIGITAL:YAU DT%' and NARRATION like '%AEPS BC COM%' or NARRATION like '%AEPS SPICE%') then AMOUNT else 0 end) as int64) as amount_credited_as_per_bank_statement,
        cast(sum(case when NARRATION like '%AEPS ACQ CR ADJ%' or narration like '%AEPS YAU CR ADJ%' then AMOUNT else 0 end) as int64) as amount_per_bank_statement_credit_adjustment_processed ,
        
        from `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
        where date(date_post) = date (@date)
        group by date3) as t3 on t1.transaction_date = t3.date3
        
        left outer join 
        
        
        (SELECT date(a.log_date_time) as date5,
        cast(count(b.spice_tid) as int64) as mini_statement_count_spice,
        FROM
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2` a
        join prod_dwh.aeps_trans_req b on a.request_id = b.request_id 
        WHERE
        DATE(a.log_date_time) = DATE (@date) AND
        date(b.log_date_time) = date (@date) AND
        b.master_trans_type = 'MS' AND
        b.aggregator = 'YBL' AND
        a.rc = '00' 
        group by date5) as t5 on t1.transaction_date = t5.date5 
        
        left join 
        
        (SELECT cast(sum(transaction_amount/100) as int64) as npci_amount1, 
        cast(sum(case when response_code = '00' and transaction_type_code = 1 then transaction_amount/100 end) as int64) as npci_amount,
        cast(count(case when response_code = '00' and transaction_type_code = 7 then rrn end) as int64) as mini_statement_count_as_per_bank, 
        cast(count(case when response_code = '00' and transaction_type_code = 7 then rrn end)*2.79*0.05 as int64) as tds_on_mini_statement_income,
        cast(count(case when response_code = '00' and transaction_type_code = 7 then rrn end)*2.79 as int64) as mini_statement_income,
        cast(count(case when response_code = '00' and transaction_type_code = 7 then rrn end)*2.79*0.10 as int64) as mini_statement_ten_percent_hold_by_bank,
        cast(count(case when response_code = '00' and transaction_type_code = 7 then rrn end)*2.79*0.18 as int64) as gst_on_mini_statement_income,
        cast(((count(case when response_code = '00' and transaction_type_code = 7 then rrn end)*2.79 + count(case when response_code = '00' and transaction_type_code = 7 then rrn end)*2.79*0.18)-count(case when response_code = '00' and transaction_type_code = 7 then rrn end)*2.79*0.05) as int64) sdl_income_gst,
        
        
        date(transaction_date) as date7
        FROM `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs` 
        where response_code = '00'  and date(transaction_date) = @date
        group by date7) as t7 on t1.transaction_date = t7.date7

        left join 

        (SELECT  cast(SUM(a.trans_amt) as int64 )as amount_detailed_logs ,count(spice_tid) as sdl_transaction_count, date(a.log_date_time) as date8
        FROM 
        `spicemoney-dwh.prod_dwh.aeps_trans_req` a 
        join
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`b
        on a.request_id = b.request_id
        
        WHERE  b.rc = '00' and a.master_trans_type = 'CW'and a.aggregator = 'YBL' and DATE(a.log_date_time) = @date 
        and date(b.log_date_time) = @date 
        group by date8) as t8 on t1.transaction_date = t8.date8)






        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])


        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)


        results = query_job.result()

        # exception_transaction

        sql_query = """
        select  date(a.log_date_time) as transaction_date
        ,a.jdt_stan, a.amount, a.rc, b.aggregator
        from prod_dwh.aeps_trans_res_v2 a
        join prod_dwh.aeps_trans_req b on a.request_id = b.request_id
        where date(a.log_date_time) = date(@date) AND
        --and a.jdt_stan='112310254378'
        date(b.log_date_time) = date(@date) 
        and b.aggregator='YBL' and a.processing_code='010000'


        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_exception_transaction', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_exception_transaction', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        # SUCCESS IN DETAILED LOGS AND NOT FOUND / FAILED IN YBL LOGS CW

        sql_query = """
        select date as trans_date , jdt_stan , spice_tid , client_id ,trans_amt as transamount ,response_code as ybl_logs_response , 
        rc as spice_response , rrn 
        from
        
        (SELECT a.spice_tid, date(a.log_date_time) as date,a.client_id , (a.trans_amt)as trans_amt,b.jdt_stan , b.rc, b.response_message ,
        a.request_id , b.request_id , 
        FROM 
        `spicemoney-dwh.prod_dwh.aeps_trans_req` a 
        join
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`b
        on a.request_id = b.request_id
        WHERE a.aggregator = 'YBL' and a.master_trans_type = 'CW' and b.rc = '00' and DATE(a.log_date_time) = "2021-09-25" 
        and date(b.log_date_time) = @date) t1
        
        left join
        
        (select rrn , response_code , transaction_amount
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs`
        where date(transaction_date) = @date and transaction_type_code = 1)t2
        
        on t1.jdt_stan = cast(t2.rrn as string)
        where (rrn is null and jdt_stan is not null) or (response_code!='00')

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_success_sdl_not_found_failed_ybl_cw', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_success_sdl_not_found_failed_ybl_cw', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        # SUCCESS IN DETAILED LOGS NOT FOUND FAILED IN YBL LOGS MS

        sql_query = """
        select date as trans_date , jdt_stan , spice_tid , client_id ,trans_amt as transamount ,response_code as ybl_logs_response , 
        rc as spice_response , rrn 
        from
        
        (SELECT a.spice_tid, date(a.log_date_time) as date,a.client_id , (a.trans_amt)as trans_amt,b.jdt_stan , b.rc, b.response_message ,
        a.request_id , b.request_id , 
        FROM 
        `spicemoney-dwh.prod_dwh.aeps_trans_req` a 
        join
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`b
        on a.request_id = b.request_id
        WHERE a.aggregator = 'YBL' and a.master_trans_type = 'MS' and b.rc = '00' and DATE(a.log_date_time) = @date 
        and date(b.log_date_time) = @date) t1
        
        left join
        
        (select rrn , response_code , transaction_amount
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs`
        where date(transaction_date) = @date and transaction_type_code = 7)t2
        
        on t1.jdt_stan = cast(t2.rrn as string)
        where (rrn is null and jdt_stan is not null) or (response_code!='00')

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_success_sdl_not_found_failed_ybl_ms', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_success_sdl_not_found_failed_ybl_ms', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        # SUCCESS IN YBL LOGS NOT FOUND IN DETAILED LOGS CW

        sql_query = """
        select rrn as bankadjref, transaction_date as date ,(transaction_amount/100 ) as adjamt,response_code as ybl_response,
        rc as spice_details_logs_response, jdt_stan as spice_rrn 
        from
        
        (SELECT a.spice_tid, date(a.log_date_time) as date,a.client_id , (a.trans_amt)as trans_amt,b.jdt_stan , b.rc, b.response_message ,
        a.request_id , b.request_id , 
        FROM 
        `spicemoney-dwh.prod_dwh.aeps_trans_req` a 
        join
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`b
        on a.request_id = b.request_id
        where
        DATE(a.log_date_time) = @date 
        and date(b.log_date_time) = @date and master_trans_type = 'CW' and a.aggregator = 'YBL') t1
        
        right join
        
        (select rrn , response_code , transaction_amount , date(transaction_date) as  transaction_date
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs`
        where date(transaction_date) = @date and response_code = '00' and transaction_type_code = 1)t2
        
        on t1.jdt_stan = cast(t2.rrn as string)
        where rrn is not null and jdt_stan is null

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_success_ybl_not_found_sdl_cw', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_success_ybl_not_found_sdl_cw', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        # SUCCESS IN YBL LOGS NOT FOUND IN SPICE DETAILED LOGS MS


        sql_query = """
        select rrn as bankadjref, transaction_date as date ,(transaction_amount/100 ) as adjamt,response_code as ybl_response,
        rc as spice_details_logs_response, jdt_stan as spice_rrn 
        from
        
        (SELECT a.spice_tid, date(a.log_date_time) as date,a.client_id , (a.trans_amt)as trans_amt,b.jdt_stan , b.rc, b.response_message ,
        a.request_id , b.request_id , 
        FROM 
        `spicemoney-dwh.prod_dwh.aeps_trans_req` a 
        join
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`b
        on a.request_id = b.request_id
        where
        DATE(a.log_date_time) = @date 
        and date(b.log_date_time) = @date and master_trans_type = 'MS' and a.aggregator = 'YBL') t1
        
        right join
        
        (select rrn , response_code , transaction_amount , date(transaction_date) as  transaction_date
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs`
        where date(transaction_date) = @date and response_code = '00' and transaction_type_code = 7)t2
        
        on t1.jdt_stan = cast(t2.rrn as string)
        where rrn is not null and jdt_stan is null

        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_success_ybl_not_found_sdl_ms', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_success_ybl_not_found_sdl_ms', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        # SUCCESS IN YBL LOGS FAILED IN DETAILED LOGS CW


        sql_query = """
        select rrn as bankadjref, transaction_date as date ,(transaction_amount) as adjamt,response_code as ybl_response,
        rc as spice_details_logs_response, jdt_stan as spice_rrn 
        
        from
        (SELECT b.jdt_stan , b.rc, a.aggregator
        FROM 
        `spicemoney-dwh.prod_dwh.aeps_trans_req` a 
        join
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`b
        on a.request_id = b.request_id
        where
        DATE(a.log_date_time) = @date 
        and date(b.log_date_time) = @date and a.aggregator = 'YBL' and master_trans_type = 'CW')t1
        
        
        right join
        
        (select rrn, response_code , transaction_amount/100 as transaction_amount ,date(transaction_date) as transaction_date
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs`
        where date(transaction_date) = @date  and transaction_type_code = 1)t2
        on t1.jdt_stan = cast(t2.rrn as string)
        where rc!='00' and response_code = '00'

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_success_ybl_failed_sdl_cw', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_success_ybl_failed_sdl_cw', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        # SUCCESS IN YBL LOGS FAILED IN SPICE DETAILED LOGS MS


        sql_query = """
        select rrn as bankadjref, transaction_date as date ,(transaction_amount ) as adjamt,response_code as ybl_response,
        rc as spice_details_logs_response, jdt_stan as spice_rrn 
        
        from
        (SELECT b.jdt_stan , b.rc, a.aggregator
        FROM 
        `spicemoney-dwh.prod_dwh.aeps_trans_req` a 
        join
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`b
        on a.request_id = b.request_id
        where
        DATE(a.log_date_time) = @date 
        and date(b.log_date_time) = @date and a.aggregator = 'YBL' and master_trans_type = 'MS')t1
        
        
        right join
        
        (select rrn, response_code , transaction_amount/100 as transaction_amount ,date(transaction_date) as transaction_date
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs`
        where date(transaction_date) = @date  and transaction_type_code = 7)t2
        on t1.jdt_stan = cast(t2.rrn as string)
        where rc!='00' and response_code = '00'

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_success_ybl_failed_sdl_ms', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_success_ybl_failed_sdl_ms', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        # SUCCESS IN YBL LOGS NOT CREDIT IN BANK STATEMENT

        sql_query = """
        select rrn ,  date , transaction_amount/100 as adjamt , cast(retailer_transaction_id as string) as bc_transaction_id,
        response_code as ybl_response_code, cwd_rrn as bank_statement_rrn , amount as bank_credit_amount
        
        from
        (SELECT * , date(transaction_date) as date
        FROM `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs`
        where date(transaction_date) = @date)a 
        
        left join
        
        (select * 
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
        where date(transaction_date)= @date)b
        
        on cast(a.rrn as string) = b.cwd_rrn
        where transaction_type_code = 1 and response_code = '00' and cwd_rrn is null

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_success_ybl_logs_not_credit_bank_statement', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_success_ybl_logs_not_credit_bank_statement', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        # CREDIT IN BANK STATEMENT NOT FOUND FAILED IN LOGS

        sql_query = """
        select cwd_rrn as bank_rrn, date ,(amount)  as adjamt , rrn as ybl_logs_rrn, response_code as ybl_response,
        amount as bank_credit_amount
        from
        
        (select * , date(a.transaction_date) as date , b.transaction_date as log_date 
        
        from 
        
        (select * 
        
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
        where date(transaction_date) = @date)a
        
        left join
        
        (select *
        
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs`
        
        where date(transaction_date) = @date) b
        on a.cwd_rrn = cast(b.rrn as string)
        where  (narration like '%AEPS ACQ CWD%' OR narration like '%Cash Withdrawal%' and rrn is null or response_code != '00'))
        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_credit_bank_statement_failed_not_found_logs', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_credit_bank_statement_failed_not_found_logs', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        # BANK AMOUNT GAP CREDIT


        sql_query = """
        select * from
        (select * , (ifnull(t1.bank_credit,0)  - ifnull(t2.spice_amount_success,0)) as difference
        from
        
        (select date(transaction_date) as transaction_date , cwd_rrn as bank_rrn , sum(amount) as bank_credit
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
        where date(transaction_date) = @date AND cwd_rrn!= '0' --and drcr_flag = 'C'
        group by transaction_date , cwd_rrn)t1
        
        left join 
        
        (select jdt_stan as spice_rrn , sum(amount) as spice_amount_success
        from 
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`a join `spicemoney-dwh.prod_dwh.aeps_trans_req`b on a.request_id = b.request_id
        where date(a.log_date_time) = @date and date(b.log_date_time) = @date and aggregator = 'YBL' and master_trans_type = 'CW' and rc = '00'
        group by jdt_stan)t2
        
        on t1.bank_rrn  = t2.spice_rrn )
        
        where difference !=0
        

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_bank_credit_amount_gap', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_credit_amount_gap', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        # SPICE SUCCESS AMOUNT GAP 

        sql_query = """
        select * from
        (select * , ( ifnull(t2.spice_amount_success,0) - ifnull(t1.bank_credit,0) ) as difference
        from
        
        (select date(a.log_date_time) as date, jdt_stan as spice_rrn , sum(amount) as spice_amount_success
        from 
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`a join `spicemoney-dwh.prod_dwh.aeps_trans_req`b on a.request_id = b.request_id
        where date(a.log_date_time) = @date and date(b.log_date_time) = @date and aggregator = 'YBL' and master_trans_type = 'CW' and rc = '00'
        group by jdt_stan , date)t2
        
        left join 
        
        (select  cwd_rrn as bank_rrn , sum(amount) as bank_credit
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
        where date(transaction_date) = @date and cwd_rrn != '0'  
        group by cwd_rrn)t1
        
        on t1.bank_rrn  = t2.spice_rrn) 
        
        where difference !=0

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_sdl_success_amount_gap', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_sdl_success_amount_gap', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        # DUPLICATE RRN IN BANK DEBIT

        sql_query = """
        SELECT transaction_date ,cwd_rrn as bank_rrn, COUNT(cwd_rrn) as bank_rrn_count , amount
        FROM `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
        where date(transaction_date) = @date and cwd_rrn!='0'and amount!=0 and drcr_flag = 'D'
        GROUP BY cwd_rrn  ,transaction_date ,amount
        HAVING COUNT(cwd_rrn)>1
        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_bank_debit_rrn_duplicacy', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_debit_rrn_duplicacy', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        # DUPLICACY IN RRN IN BANK CREDIT

        sql_query = """
        SELECT transaction_date ,cwd_rrn as bank_rrn, COUNT(cwd_rrn) as bank_rrn_count , amount
        FROM `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
        where date(transaction_date) = @date and cwd_rrn!='0'and amount!=0 and drcr_flag = 'C'
        GROUP BY cwd_rrn  ,transaction_date ,amount
        HAVING COUNT(cwd_rrn)>1

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_bank_credit_rrn_duplicacy', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_credit_rrn_duplicacy', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        # YBL LOGS RRN DUPLICACY

        sql_query = """
        SELECT date(transaction_date) as date, rrn , count(rrn) as count_of_rrn , transaction_amount
        FROM `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs` 
        where date(transaction_date) = @date and rrn!= 0  and transaction_type_code = 1
        group by  rrn,date , transaction_amount
        having count(rrn)>1


        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs_rrn_duplicacy', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs_rrn_duplicacy', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        # JDT STAN DUPLICACY

        sql_query = """
        SELECT date(a.log_date_time) as date, jdt_stan, COUNT(jdt_stan) as total_count , amount
        FROM `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`a
        join
        `spicemoney-dwh.prod_dwh.aeps_trans_req`b
        on a.request_id = b.request_id
        where date(a.log_date_time) = @date and date(b.log_date_time) = @date and rc='00' and master_trans_type = 'CW' and aggregator = 'YBL'
        GROUP BY jdt_stan , date , amount
        HAVING COUNT(jdt_stan)>1
        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_jdt_stan_duplicacy', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_jdt_stan_duplicacy', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        # INCORRECT PARTICULAR
        
        sql_query = """
        select * from
        (SELECT
        *,
        REGEXP_CONTAINS(substring(narration,22,12),r'[0-9]{12}$') AS is_valid
        FROM `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
        where narration like 'AEPS/Cash Withdrawal/%' and date(transaction_date) = @date)
        where is_valid is false

        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_incorrect_particular', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_incorrect_particular', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        # SUCCESS IN LOGS BUT DEBIT IN BANK STATEMENT
        
        sql_query = """  

        select * from
        (select date(transaction_date) as date , cast(transaction_amount/100 as int64) as transaction_amount , rrn , response_code, retailer_transaction_id
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs`
        where date(transaction_date) = @date and response_code = '00')t1
        
        join
        
        (select cwd_rrn , drcr_flag
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_bank_statement`
        where date(transaction_date) = @date and drcr_flag!='C'
        )t2
        
        on cast(t1.rrn as string) = t2.cwd_rrn

        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_success_logs_debit_bank_statement', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_success_logs_debit_bank_statement', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        # CREDIT ADJUSTMENT
        
        
        sql_query = """

        select 
        rrn as bankadjref, 'C' as flag , transaction_date  as shtdat , (t2.transaction_amount/100) as amount, rrn as shser , iin as shcrd , 'Spice' as filename , '1' as reason , retailer_transaction_id as spice_tid
        from
        
        (SELECT a.spice_tid, date(a.log_date_time) as date,a.client_id , (a.trans_amt)as trans_amt,b.jdt_stan , b.rc, b.response_message ,
        a.request_id , b.request_id 
        FROM 
        `spicemoney-dwh.prod_dwh.aeps_trans_req` a 
        join
        `spicemoney-dwh.prod_dwh.aeps_trans_res_v2`b
        on a.request_id = b.request_id
        where
        DATE(a.log_date_time) = @date 
        and date(b.log_date_time) = @date and master_trans_type = 'CW'and a.aggregator = 'YBL') t1
        
        right join
        
        (select rrn , response_code , transaction_amount , date(transaction_date) as  transaction_date , retailer_transaction_id , iin
        from 
        `spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_logs`
        where date(transaction_date) = @date and response_code = '00' and transaction_type_code = 1)t2
        
        on t1.jdt_stan = cast(t2.rrn as string)
        where (rrn is not null and jdt_stan is null) or rc!='00'

        """

        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_aeps_ybl_credit_adjustment', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_aeps_ybl_credit_adjustment', write_disposition='WRITE_APPEND' ,  query_parameters=[
        bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()



        print('Recon Success for {} at {}'.format(dates, times))
        with open('/home/sdlreco/crons/ybl_aeps/stat/stat-'+str(dates)+'.txt', 'w') as f:
            f.write('1')
            f.close()

#driver

import sys
sys.path.insert(0, '/home/sdlreco/crons/smarten/')
import payload as smarten

ybl_aeps = ['17ca00e7e1b', '17ca0109a31', '17ca0128de1', '17ca014a9d5', '17d75829e4f', '17ca0172733', '17ca01a75aa', '17ca01df43e', '17ca020b2ba',
            '17ca022bfc0', '17ca024f837', '17ca02782b4', '17ca02a4837', '17ca02c8216', '17ca02f39b7', '17ca031bb07', '17ca034732d', '17ca03753ba', '17ca039fe70']

with open('/home/sdlreco/crons/ybl_aeps/stat/stat-'+str(dates)+'.txt', 'r') as f:
    lines = f.read().splitlines()
    f.close()
    if('1' in lines):
        print('Tried at {}, but reco already done !!'.format(times))
        pass
    else:
        main()
        for ids in ybl_aeps:

            smarten.payload(ids)
        print('Refreshed : ybl_aeps')
