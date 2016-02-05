'''
Created on Jun 22, 2012

@author: eocampo

Use for Standalone applications
20140106 EO Added qrys for predecessors.
20140307 EO changed CPM query.
20140821 EO changed CPM query. Added lastworkday qry.
20150206 EO Added selWkfDateStatus mostly for predecessors.
20150310 EO Added selFinRCOpenROPOQry and rn selMssqlPredDlyQry to selMssqlPredQry
'''
__version__ = '20150310'

import sys

# Todays WorkDay
workDay     = "SELECT Mon,Yr,Date_Day,Work_Day FROM EDW_WORK_DAY WHERE Date_Day = current_date"

lastworkDay = """ SELECT Date_Day FROM  EDW_WORK_DAY  WHERE  mon = %s and yr = %s 
                         AND work_day = (SELECT MAX(work_day) from EDW_WORK_DAY  WHERE mon = %s and yr = %s) - %s """
# 
# Predecessors:
selInfPredDlyQry = """ select to_char(max(start_time),'YYYYMMDD' ) from REP_WFLOW_RUN
                   where workflow_name = '%s' and run_err_code = 0
                   group by workflow_name order by 1 """


selMssqlPredQry =  """ select left(max(run_date),8) run_date from msdb.dbo.sysjobhistory a , msdb.dbo.sysjobs b
                        where a.job_id = b.job_id
                        and b.name = '%s'
                        and a.step_id  = 0
                        and a.run_status = 1"""

# This query returns wkf_name and last date run in pairs.( 1 or more)
# Invoke DBInfaAdmin.selWkfDateStatus % ('w1','w2', ...'wn')
selWkfDateStatus  =  """SELECT workflow_name , to_char(max(start_time),'YYYYMM' ) FROM rep_wflow_run
                        WHERE workflow_name in (%s)
                        AND run_err_code = 0
                        GROUP BY workflow_name ORDER by 1 """ 

# InterProcess Handshaking (Job Control)
selJobCtrlStatus = """SELECT job_run_state , job_name , job_run_msg, job_run_date
                      FROM  job_ctrl 
                      WHERE job_id = %s"""     
                      
updJobCtrlStatus = """UPDATE JOB_CTRL
                      set job_run_state = %s,
                      job_run_msg  = %s
                      where job_id = %s """ 
                      
# Beerstore 
# Find unmatch vehicles.
beerStUnmatchVehSql = \
""" SELECT distinct d11.hdr_ryder_fis_customer_number hdr_ryder_fis_customer_number,cast(d11.vehicle_number as varchar(50)) ryder_veh_nbr
   FROM dw_stg..fis_stg_inv901_d11_repair_order_header d11
   JOIN edw_idl..ctrl_elctr_inv_trdng_prtnr_fiscust_nbr tp ON
   CAST(d11.hdr_ryder_fis_customer_number AS integer) = CAST(tp.rydr_fiscust_nbr AS integer)
   LEFT JOIN edw_out..elctr_inv_load_mnfst lm ON lm.inv_nbr = d11.invoice_number
   AND CAST(lm.rydr_fiscust_nbr AS integer) = CAST(d11.hdr_ryder_fis_customer_number AS integer)
   LEFT JOIN edw_idl..ext_elctr_inv_trdng_prtnr_xref XREF ON (cast(d11.vehicle_number as varchar(50)) = xref.trdng_prtnr_veh_nbr)
   AND (to_number(tp.rydr_fiscust_nbr,'999999999') = XREF.rydr_fiscust_nbr)
   WHERE lm.inv_nbr IS NULL
   AND xref.trdng_prtnr_veh_nbr is null
   AND d11.hdr_invoice_date > '08/22/2013'  
   ORDER BY 2 """

# CPM
selCPMPartListCnt = "SELECT run_id, load_src_cd , count(*)  FROM  tblEDWVendorDetails GROUP BY run_id, load_src_cd"
selCMPUpdEdition  = "SELECT vwedition, country_cd, rowcount FROM tbledwcpm_cnt"

#selCMPUpdEdition  = \
#""" SELECT vwedition,
#        CASE when country_cd=1 then 'USA' else 'CAN' end Country,
#        count(*) Count
#    FROM dbo.tblEDWCPM
#    WHERE proc_dt>=DATEADD(dd,-1,DATEDIFF(dd,0,GETDATE()))
#    GROUP BY  vwedition,country_cd,proc_dt """

# Finance detail
selFinCntlRecCnt = \
"""  SELECT etl_proc_cnt FROM fin_cntl_rec WHERE etl_proc_nm= 'DB2PRD.%s' 
     AND etl_proc_ts = ( SELECT max(etl_proc_ts) FROM fin_cntl_rec  WHERE etl_proc_nm= 'DB2PRD.%s') """
     
     
selFinRCOpenROPOQry = \
"""  SELECT Table_Name, convert(varchar(8),Etl_dt,112),Nbr_Rows
     FROM smo_prod_data.dbo.Etl_Log with (nolock)
     WHERE table_name = '%s'
     AND Etl_Dt >= convert(varchar(10),getdate(),101) """

# Invoice
selInvAppDtlLoadDtQry = "SELECT count(1) FROM inv_stg_scuaphis WHERE processdt = to_date('%s','mm/dd/yyyy')"
     
##Lease Credit
#updLCRStgCortExtQry = """  UPDATE lcr_stg_cortera_extract  SET LOAD_DT = :load_dt
#                           WHERE LOAD_DT = :curr_dt """
                           
#Lease Credit
updLCRStgCortExtQry = """  UPDATE lcr_stg_cortera_extract  SET LOAD_DT = '%s'
                           WHERE LOAD_DT = '%s' """

#OPNL CTL 
selFinTranMthTotQry = """ SELECT eff_dt ,count(1)
                          FROM   fin_act_post_trn
                          WHERE  eff_dt  BETWEEN to_date('%s','mm/dd/yyyy') AND 
                          last_day(to_date('%s','mm/dd/yyyy'))
                          GROUP by eff_dt
                          ORDER  BY eff_dt"""


selFinTranMthDupQry = """ SELECT DUP.EFF_DT , COUNT(1)
                          FROM (SELECT eff_dt , COUNT(1) AS Cnt FROM fin_act_post_trn
                          WHERE  eff_dt  BETWEEN to_date('%s','mm/dd/yyyy')  
                          AND last_day(to_date('%s','mm/dd/yyyy'))
                          GROUP BY cntl_enty_cd, acct_cd, crnc_cd, cmmn_acct_cd, bsns_unit_cd, loc_cd, actg_dt, actg_dt_int, actg_yr, 
                                   actg_mo, actg_yr_mo, actg_yr_mo_int, fnct_cd, pl_cd, src_crnc_cd, src_cd, aplc_cd, aplc_refc_cd, veh_no, ven_no, cust_no, 
                                   jegrpxnbr, jegrpdt, jexnbr, jeseq, legacy_jrnl_unq_id, legacy_jrnl_unq_sq, proc_dt, proc_tm, trn_am, je_ds, src_tbl, loc_iss_cd, 
                                   ickval, invcdistlinenbr, eventcd, subeventcd, actvycd, pmtnbr, avail_dt, eff_dt, expr_dt, aptracker_url
                          HAVING Cnt > 1
                          ) AS DUP
                         GROUP BY DUP.EFF_DT
                         ORDER BY DUP.eff_dt DESC"""
                         
# Peg Billing PreCheck
rprOrderSql  = \
""" SELECT etl_load_dt,count(1)
    FROM   rpr_peg_repair_order
    WHERE etl_load_dt BETWEEN to_date('%s','mm/dd/yyyy') AND 
          last_day(to_date('%s','mm/dd/yyyy'))
    GROUP by etl_load_dt
    ORDER  BY etl_load_dt""" 
    
rprGralLedgerSql = \
""" SELECT etl_load_dt,count(1)
    FROM   rpr_peg_general_ledger
    WHERE etl_load_dt BETWEEN to_date('%s','mm/dd/yyyy') AND 
          last_day(to_date('%s','mm/dd/yyyy'))
    GROUP by etl_load_dt
    ORDER  BY etl_load_dt""" 

# Peg Billing Post Check
# Issue with UNIXODBC with data type Numeric.
if(sys.platform == 'win32'): 
    selPegBillMthTotQry = \
    """ SELECT source, sum(total_cost), count(*) 
        FROM rpr_peg_stg_detail_all
        WHERE source != 'GL'
        GROUP BY source
        ORDER BY source """
else:
    selPegBillMthTotQry = \
    """ SELECT source, CAST(sum(total_cost) AS DOUBLE), count(*) 
        FROM rpr_peg_stg_detail_all
        WHERE source != 'GL'
        GROUP BY source
        ORDER BY source """
    
selPegBillMthDeanDetQry = \
""" SELECT peg_id,audit_dt,audit_tm,cost_period,lessee_no,customer_no,customer_name,veh_no,odom,
       dom_loc,dom_loc_name,iss_loc,iss_loc_name,source,po_no,ro_no,repair_dt,ro_type,rpr_reason,
       rpr_rsn_desc,exclusion_type,line_item,gl_acct_no,part_ata_desc,part_system,part_assembly,
       part_component,part_no,part_desc,ata_desc,system,assembly,component,action,action_desc,posn,
       part_qty,part_amt,labor_hours,labor_amt,overhead_cost,outside_cost,other_cost,oil_cost,
       tire_cost,total_cost,proc_dt,proc_time,proc_fg,etl_load_dt
    FROM rpr_peg_stg_detail_all""" 


                     





