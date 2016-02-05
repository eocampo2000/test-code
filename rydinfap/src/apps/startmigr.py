'''
Created on Jul 7, 2014

'''

import os , sys
import csv
import ConfigParser
import subprocess
from optparse import OptionParser

import proc.process    as proc
import utils.fileutils as fu
import utils.strutils  as su
import common.log4py   as log4py 
from   common.loghdl import getLogHandler

ln   =  'gp2nz_mig' 
log  =  log4py.Logger().get_instance()
logfile = getLogHandler(ln,log)
tblLoadCnt = {'table1':100,
              'table2':200,
              'table3':300,
              'table4':1,
              'table5':5,
              }  # Hold target load counts.

def getOptions(arg):
    usage = 'usage: %prog [options -t=<table name>]'
    parser = OptionParser(usage)
    parser.add_option('-t', '--table', dest='table', type='string', metavar='name',
            help='Table to be extracted')
    parser.add_option('-c', '--config', dest='config', type='string', metavar='name',
            help='Config file name')
    (options, arg) = parser.parse_args(arg)
    parser.destroy()
    return (options, arg)  

def setEnvDir(options):
    args = {}
    tbl_samp_row = 0
    tbl_samp_pct = 0
    # Env vars.
    try:
        args['gp_nz_home'] = os.environ['GP_NZ_HOME' ]
        # GP
        args['gp_db_host'] = os.environ['GP_DB_HOST' ]
        args['gp_db_user'] = os.environ['GP_DB_USER' ]
        args['gp_db_port'] = os.environ['GP_DB_PORT' ]
        args['gp_db_env' ] = os.environ['GP_DB_ENV'  ] 
        args['gp_db_sche'] = os.environ['GP_DB_SCHE' ] 
        #NZ    
        args['nz_db_env' ] = os.environ['NZ_DB_ENV'  ]
        args['nz_db_host'] = os.environ['NZ_DB_HOST' ]
        args['nz_db_user'] = os.environ['NZ_DB_USER' ]
        args['nz_db_pwd' ] = os.environ['NZ_DB_PWD'  ]
        args['nz_db_port'] = os.environ['NZ_DB_PORT' ]

        #Verification of result sets
        if os.environ.has_key('TBL_SAMP_PCT'): tbl_samp_pct = su.toInt(os.environ['TBL_SAMP_PCT'])
        args['tbl_samp_pct'] = tbl_samp_pct
        if os.environ.has_key('TBL_SAMP_ROW'): tbl_samp_row = su.toInt(os.environ['TBL_SAMP_ROW'])
        args['tbl_samp_row'] = tbl_samp_row
        if tbl_samp_pct > 0 and tbl_samp_row:
            log.error('TBL_SAMP_PCT = %s TBL_SAMP_ROW = %s are Mutually exclusive param.\nWill do sampling :  %s %% .' % (tbl_samp_pct,tbl_samp_row,tbl_samp_pct))

        if options.config != None:
            args['config'] = options.config
        else:
            args['config'] = '%s/config/gp_table.ini'%(args['gp_nz_home'])
    
        # Directory settings.   
        args['data'      ] = '%s/data'   %(args['gp_nz_home'])
        args['log'       ] = '%s/log'    %(args['gp_nz_home'])
        args['script'    ] = '%s/script' %(args['gp_nz_home'])
        args['nzlog'     ] = '%s/nzlog'  %(args['gp_nz_home'])
        args['nzbad'     ] = '%s/nzbad'  %(args['gp_nz_home'])
    
    except:
       log.error("ENV var not set %s %s\n" % (sys.exc_type, sys.exc_info()[1]))
    
    finally:
       log.debug('args %s ' % args)
       return args

def printEnv(args):
    if isinstance(args, dict):
        for k, v in sorted(args.items()):
            log.info( u'{0}:\t{1}'.format(k, v))
    
def getDefinitionsVariable(config_file):
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    table_list = []
    for section in config.sections():
       print section, dir(section)
    return table_list
    
def readConfigFile(config_file):
    f = fu.openFile(config_file, 'r')
    if f is not None : return f.read().splitlines() 
    else             : return None
    
def getTblList(args,options):
    tl = []
    if options.table != None:
       tl.append(options.table) 
    else:
       cfl = args['config']        
       tl = readConfigFile(cfl)

    if tl is None :
        log.error('Issue opening config file %s ' % cfl)
        return []
    return tl

# By convention filename is SCH_validation.lst
def getAggColList(args):
    fnp = os.path.join(args['gp_nz_home'],'config', '%s_validation.lst' % args['gp_db_sche']) 
    tb_pk = []
    f = fu.openFile(fnp,'r') 
    if f is None : 
        log.error('Cannot open validation file %s ' % fnp)
        return tb_pk

    log.info('read validation file %s ' % fnp)  
    rd = csv.reader(f)

    for l in rd:
       log.debug('tb,pk',l)
       if len(l) != 2 :
           log.error("Error ignoring ", l ," len needs to be 2")
           continue
       tb_pk.append(l)
 
    f.close()
    return tb_pk

def getRowLimit(args,table_name):
    
    if not(tblLoadCnt.has_key(table_name)):
        log.error('table %s has not been loaded' % table_name)
        return -1
    
    if args['tbl_samp_pct'] > 0 : 
        pct = args['tbl_samp_pct']
        cnt = tblLoadCnt[table_name]
        if (pct < 1 or cnt < 1) :
            log.error('pct is %s cnt is %s ' % (pct,cnt))
            return -2
        
        rl = cnt * pct / 100
        if rl < 1 : rl = cnt
        log.debug('PCT -- tbl name=%s pct = %d cnt = %d row limit = %d' % (table_name,pct,cnt,rl))
        return rl
        
    if args['tbl_samp_row'] > 0 :
        rl = args['tbl_samp_row']
        log.debug('ROW_LIM --tbl name=%s row limit = %d' % (table_name,rl))
        return rl

    return -3    
    
# DB DML operations
def _getGPCnt(args,table_name):
    # Get number of records in GP table
    cmd = ["psql  -h  %s -p %s -U %s -d %s -c 'select count(1) from %s.%s' |head -3 |tail -1" % (args['gp_db_host'],
                                                                                          args['gp_db_port'],
                                                                                          args['gp_db_user'],
                                                                                          args['gp_db_env'],
                                                                                          args['gp_db_sche'],
                                                                                          table_name,)]
    rc,rmsg = proc.runSync(cmd,log)
    if rc == 0 :
        log.debug ('cmd %s -> msg= %s' % (cmd,rmsg) )
        return rmsg.strip()
    else       :
        log.error('cmd %s rc = %s -> msg= %s' % (cmd,rc,rmsg))
        return -1

def _getNZCnt(args,table_name):
    # Get number of records in NZ table
    cmd =  [" nzsql  -host %s -db %s -u %s -pw %s  -c 'select count(1) from  %s..%s' |head -3|tail -1" %(args['nz_db_host'],
                                                                                                               args['nz_db_env'],
                                                                                                               args['nz_db_user'],
                                                                                                               args['nz_db_pwd'],
                                                                                                               args['nz_db_env'],
                                                                                                               table_name)]
    rc,rmsg = proc.runSync(cmd, log)
    if rc == 0 : 
        log.debug ('cmd %s -> msg= %s' % (cmd,rmsg) )
        return rmsg.strip()
    else       : 
        log.error('cmd %s rc = %s -> msg= %s' % (cmd,rc,rmsg))
        return -1

def _delNZRows(args,table_name):

    cmd =  [" nzsql -host %s -db %s -u %s -pw %s -c 'delete from %s..%s'" % (args['nz_db_host'],
                                                                             args['nz_db_env'],
                                                                             args['nz_db_user'],
                                                                             args['nz_db_pwd'],
                                                                             args['nz_db_env'],
                                                                             table_name)]
    rc,rmsg = proc.runSync(cmd, log)
    if rc == 0 : log.debug ('cmd %s -> msg= %s' % (cmd,rmsg) )
    else       : log.error('cmd %s rc = %s -> msg= %s' % (cmd,rc,rmsg))
    return rc


def _getGPSum(args,table_name,col_name):
    # Get number of records in GP table
    cmd = ["psql  -h  %s -p %s -U %s -d %s -c 'select sum(%s) from %s.%s' |head -3 |tail -1" % (args['gp_db_host'],
                                                                                          args['gp_db_port'],
                                                                                          args['gp_db_user'],
                                                                                          args['gp_db_env'],
                                                                                          col_name,
                                                                                          args['gp_db_sche'],
                                                                                          table_name,)]
    rc,rmsg = proc.runSync(cmd,log)
    if rc == 0 :
        log.debug ('cmd %s -> msg= %s' % (cmd,rmsg) )
        return rmsg.strip()
    else       :
        log.error('cmd %s rc = %s -> msg= %s' % (cmd,rc,rmsg))
        return -1

def _getNZSum(args,table_name,col_name):
    # Get number of records in NZ table
    cmd =  [" nzsql  -host %s -db %s -u %s -pw %s  -c 'select sum(%s) from  %s..%s' |head -3|tail -1" %(args['nz_db_host'],
                                                                                                        args['nz_db_env'],
                                                                                                        args['nz_db_user'],
                                                                                                        args['nz_db_pwd'],
                                                                                                        col_name,
                                                                                                        args['nz_db_env'],
                                                                                                        table_name)]
    rc,rmsg = proc.runSync(cmd, log)
    if rc == 0 :
        log.debug ('cmd %s -> msg= %s' % (cmd,rmsg) )
        return rmsg.strip()
    else       :
        log.error('cmd %s rc = %s -> msg= %s' % (cmd,rc,rmsg))
        return -1


def bld_nzsql_cmd(args,table_name):
    log.debug('table_name = %s' % (table_name))
    sql_c = ''' nzsql  -host %s -db %s -u %s -pw %s -c "SELECT CASE 
               WHEN attnum=1 THEN 'select '|| col_nm 
               WHEN attnum=last_col THEN ',' || col_nm || ' from '|| substr(DATABASE,1,instr(DATABASE,'_',1)-1)||'.'|| name 
               ELSE CASE 
                WHEN 1=1 THEN ','||col_nm 
                ELSE ','||col_nm 
                END 
           END sql_q
        FROM 
          ( SELECT CASE 
               WHEN (instr(format_type,'character',1) > 0
                 OR instr(format_type,'text',1) > 0) THEN 'regexp_replace(regexp_replace(regexp_replace('||attname||',chr(92)||chr(92),chr(92)||chr(92)||chr(92),''g''),chr(10),chr(92)||chr(10),''g''),chr(13),chr(92)||chr(13),''g'')' 
               WHEN (instr(format_type,'boolean',1) > 0) THEN 'case when '||attname||'=''t'' then 1 when '||attname||'=''f'' then 0 else null end ' 
               ELSE attname 
               END col_nm ,
               name, 
               attnum,
               last_col,
               DATABASE, 
               format_type 
           FROM _V_RELATION_COLUMN e, 
         (SELECT max(attnum) last_col
          FROM _V_RELATION_COLUMN e 
          WHERE DATABASE ='%s'
            AND name='%s')c 
           WHERE DATABASE ='%s'
         AND name='%s')s 
        ORDER BY attnum" ''' % (args['nz_db_host'] ,
                    args['nz_db_env'],
                    args['nz_db_user'], 
                    args['nz_db_pwd' ], 
                    args['nz_db_env'],
                    table_name,
                    args['nz_db_env'],
                    table_name)    
    return sql_c

def _crtSchemaScript(args,schema_script,table_name):
    
    # Create NZ script to replace existing non print ascii
    sql_c= bld_nzsql_cmd(args,table_name)
    log.debug("sql_c = %s" % sql_c)
       
    rc = fu.createFile(schema_script,sql_c)    
    if rc == 0 : os.chmod(schema_script , 0777)         
    return rc
           
# This method will create the final gp_export cmd to be PIPED to nzload.
# Creates export_script e.g. exp_sdl.aps_stg_spcases.sql
def _runGPExpScript(args,schema_script,export_script,table_name):
    retVal=0   
    sql_cmd=[schema_script]
    log.debug('schema_script = %s' % schema_script)
    schema_script_gp = os.path.join(args['script'],'schm_'+ args['gp_db_sche']+ '.' + table_name+'_gp.sql')
    try:
        s_f_gp = open(schema_script_gp, 'w')

        sql_proc = subprocess.Popen(sql_cmd, shell=True , stdout= s_f_gp)
        sql_proc.communicate()

        sql_txt = ['cat %s |tail -n +3|head -n -2'%(schema_script_gp)]

        sql_gp = subprocess.Popen(sql_txt, shell=True , stdout=subprocess.PIPE)
        stdout = sql_gp.communicate()[0]
   
        gp_cmd = "psql  -h  %s -p %s -U %s -d %s <<EOSQL\nCOPY (%s ) to STDOUT WITH   DELIMITER ',' NULL 'NULL' ESCAPE '\"' CSV QUOTE E'\"';\nEOSQL" %(
                                                                                                                     args['gp_db_host'],
                                                                                                                     args['gp_db_port'],
                                                                                                                     args['gp_db_user'],
                                                                                                                     args['gp_db_env'], stdout)
        log.debug("schema_script_gp  = %s " % schema_script_gp)
        log.debug('gp_cmd = %s ' % gp_cmd) 
        f = open(export_script,'w')
        f.write( gp_cmd )
        f.close()
   
        os.chmod(export_script, 0777)

    except:
        log.error("==EXCEP %s %s" % (sys.exc_type,sys.exc_value))
        retVal = 1
    
    finally:
        return retVal 

# This method starts the actual load nzload(NZ) | plsql (GP)
def _runNZImpScript(args,export_script,table_name):
    retVal=0
    cmd = [export_script]
    try:
       exp_proc = subprocess.Popen(cmd, shell=True , stdout=subprocess.PIPE)

       import_script= os.path.join(args['script'],'imp_'+ args['gp_db_sche']+ '.' + table_name+'.sql')
       log.debug('Import begins ... import_Script = %s ' % import_script)

       imp_cmd="cat %s | nzload -host sscptdnetezza2a -db %s -u admin -pw password -t %s -lf '%s/%s.log' -skipRows '0' -delim ','  -dateStyle  'YMD' "\
               "-timeStyle '24HOUR' -dateDelim '-' -nullValue 'NULL'  -CrInString -ctrlChars -maxErrors '10000' -EscapeChar '\\' -quotedValue "\
               " 'DOUBLE' -bf '%s/%s.nzbad'" %('/dev/fd/'+str(exp_proc.stdout.fileno()), 
                                                args['nz_db_env'], table_name, 
                                                args['nzlog'], table_name, args['nzbad'], table_name)
       log.debug('Import Cmd = %s ' % imp_cmd)
       
       f = open(import_script,'w')
       f.write(imp_cmd)
       f.close()
       os.chmod(import_script, 0777)

       cmd = [import_script ]
  
       lf = os.path.join(args['nzlog'],args['gp_db_sche']+ '.' + table_name +'.log')
       log.debug('logfile = %s ' % lf)
       if os.path.exists(lf):
         os.remove(lf)

       imp_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
       stdout = imp_proc.communicate()[0]

    except:
        log.error("==EXCEP %s %s" % (sys.exc_type,sys.exc_value))
        retVal = 1

    finally:
        return retVal

def chkDataInty(args,table_list):
    print '%s ' % tblLoadCnt
    rc = 0
    for table_name in table_list:
        log.info('Comparing GP->NZ table_name %s ' % table_name)
        #table_name = table.split('.')[1]
      
        row_limit = getRowLimit(args,table_name)  
        log.info('Comparing GP->NZ table_name %s row_limit = %d ' % (table_name, row_limit))
        #Get table pk from NZ Catalog.
        nz_cmd = 'nzsql -host %s -db %s -u %s -pw %s -c "select * from %s order by %s limit %d"' %(args['nz_db_host'],
                                                                                                    args['nz_db_env'],
                                                                                                    args['nz_db_user'],
                                                                                                    args['nz_db_pwd'],
                                                                                                    table_name,
                                                                                                    'pk_column',
                                                                                                    row_limit)
        
        log.info('nz_cmd is %s ' % nz_cmd)
 # ac aggregation column
def chkDataAggr(args,tb_ac):
    rc = 0
    log.info('Checking aggregates for %d tables' % len(tb_ac))
    for tb,ac in tb_ac:
       log.debug('Table = %s Aggr Col = %s' % (tb,ac))
       if len(ac.strip()) < 1 :
           log.error('table %s invalid Aggr Col %s' % (tb,ac))
           rc+=1
           continue
       
       tgt_out = _getNZSum(args,tb,ac)
       tgt_sum = su.toFloat(tgt_out)
       log.debug('tgt_out = %s tgt_sum %s' % (tgt_out,tgt_sum))
       src_out = _getGPSum(args,tb,ac)
       src_sum = su.toFloat(src_out)
       log.debug('src_out = %s src_sum %s' % (src_out,src_sum))
       if (tgt_sum is None or src_sum is None):
          log.error('Got Inv Number for tbl = %s aggr col = %s tgt_sum = %s  src_sum = %s ' % (tb,ac, tgt_sum,src_sum))
          continue
       d = tgt_sum - src_sum
       if d != 0.0:
          log.error('tbl = %s aggr col = %s diff = %s ' % (tb,ac,d))
          log.error('GP = %s\t NZ = %s ' % (src_sum,tgt_sum))
          rc+=1
       else : log.info('SUM OK for tbl = %s aggr col = %s' % (tb,ac)) 
    return rc   
       
def start_load(args,table_list):
    
    for t in table_list:
       table_name = t.strip(' \t\n\r')
       export_script = os.path.join(args['script'],'exp_'+ args['gp_db_sche'] +  '.' + table_name + '.sql')
       data_filename = os.path.join(args['data'], args['gp_db_sche'] + '.' + table_name + '.csv')
       log_filename  = os.path.join(args['nzlog'], args['gp_db_sche'] + '.' + table_name + '.log')

       log.debug ('export_script = %s\ndata_filename = %s\ntable_name = %s\nlog_filename = %s log_filename' % (export_script,
                                                                                                               data_filename,
                                                                                                               table_name,
                                                                                                               log_filename))
       source_count = _getGPCnt(args,table_name)
       if su.toInt(source_count) < 1 : 
          log.warn('No rows to load from GP %s.%s ' % (args['gp_db_sche'],table_name))
          continue
       log.info('GP Source Table %s\tcount=%s' % (table_name,source_count))

       schema_script = os.path.join(args['script'],'schm_' + args['gp_db_sche'] + '.' + table_name + '.sql')
       rc =_crtSchemaScript(args,schema_script,table_name)
       if rc == 0 :
           log.debug('Created Schema script %s' % (schema_script))
       else :
           log.error('Creating Schema script %s' % (schema_script))
           continue

       rc = _runGPExpScript(args,schema_script,export_script,table_name) 

       nz_cnt = _getNZCnt(args,table_name)
       log.debug('Record count in Netezza (pre-import) = %s'%(nz_cnt))
      
       if su.toInt(nz_cnt)   > 0 :

           rc = _delNZRows(args,table_name)
           if rc == 0 : log.debug('Table %s has been truncated..' % table_name)
           else       : log.error('Table %s cannot be truncated..' % table_name)

           nz_cnt = _getNZCnt(args,table_name)
           log.debug('Record count in Netezza after delete = %s'%(nz_cnt))

       if export_script == '/infa/product/pc951/server/infa_shared_conv/SrcFiles/gp_nz_migration/script/exp_sdl.aps_stg_spcases.sql':
          export_script = '/infa/product/pc951/server/infa_shared_conv/SrcFiles/gp_nz_migration/script/exp_sdl.aps_stg_spcases_bkp.sql'

       log.debug('export_script = %s' % export_script) 
       rc = _runNZImpScript(args,export_script,table_name)

       target_count = _getNZCnt(args,table_name)
       log.info('NZ Target Table %s\tcount=%s'%(table_name,target_count))

       if source_count <> target_count:
         log.error('Loading %s. Please check log %s'%(args['gp_db_sche'] + table_name ,log_filename))
       else:
         log.debug('Loaded successfully')
    
    return 0    # EO Need to add .

def main():
    
        (options, arg) = getOptions(sys.argv[1:])  
        args = setEnvDir(options)
        if len(args) < 12 : 
            log.error('Need to set all env variables. Only Set %s' % args)
            sys.exit(1)
    
        printEnv(args)
        #table_list = getTblList(args,options)
        table_list = ('table1','table2','table3','table4','table5','table6')
        log.debug ('table_list = ', table_list)
        if len(table_list) < 1 : 
            log.error('There are no tables to process. Exiting')
            sys.exit(2)
        
        
#        sys.exit(3)
#        rc = start_load(args,table_list)
#        log.info('start_load rc = %s' % rc)

        rc = chkDataInty(args,table_list)
        log.info('chkDataInty rc = %s' % rc)
        sys.exit(3)

        ac_list = getAggColList(args)
        if len(ac_list) < 1 :
            log.error('There are no tables/ac to process. Exiting')
            sys.exit(3)
 
        rc = chkDataAggr(args,ac_list)
        log.info('chkDataAggr rc = %s' % rc)
        return rc
    
if __name__ == '__main__':
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()         # Remove in UX 
    log.info('start_load starting PID = %s' % os.getpid())
    rc =  main()
    log.info('start_load completed\trc = %s PID = %s' % (rc,os.getpid()))
    sys.exit(rc)
