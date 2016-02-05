#===============================================================================
# Version 1.0  20090807
#
# render.py
# 
# Creation Date:     
# 
# Description: This module contains report/output rendering classes and methods.
#              It is specific to the softinv application.            
# 
# EO : 20110401 -> Added class "pwd" 
# $SOFT_DEPO = \\goxsf101\IMDS$\Data Warehouse\Informatica
# EO : 20110406 -> Added EOL clas and change href to popup window.    
#===============================================================================\
import sys

#--------------  EO Modified for web2py. Fully qualify the package:
#import applications.softinv.modules.procdata.procsoftinv as psi
#import applications.softinv.modules.mainglob as mg
#--------------  End of Modification 

#import procdata.procsoftinv as psi
import mainglob as mg

# Render HTLM base class.
# Use mostly for StandAlone html's 
class RenderHTML:
    
    def bldHtmlHeader(self):
        strList = []
        strList.append('<html><head><style type="text/css">')
        strList.append('h2{text-align: center}')
        strList.append('tr.d0 td {background-color:#F0F0F0; color: black;} tr.d1 td {background-color:#DCDCDC; color: black;}')
        #strList.append('table.envdisp {border-width: 1px; border-style: outset;border-color: gray;border-collapse: collapse;margin-left: 10%;width: 60%;}')
        strList.append('table.envdisp {border-width: 1px; border-style: outset;border-color: gray;border-collapse: collapse;width: 80%;}')
        strList.append('table.envdisp th { border-width: 1px;border-color: gray; text-align: left;}')
        strList.append('table.envdisp td { border-width: 1px;border-style: inset;border-color: gray;}')
        strList.append('#hilite { background-color: #99CCFF;font-weight:bold;color: blue;}')
        strList.append('</style></head><body>')
        return ''.join(strList)
    

    def bldHtmlFooter(self):
        return ('<br> </body></html>')
    
# Class for PC 
class InfaPCHTML(RenderHTML):
      
    # imgDir : Image directory
    # vendor : Vendor Name e.g Informatica
    
    def __init__(self,imgDir):
        self.imgDir = imgDir
        self.scrd   = mg.infa_pc_rs
     
    # htmlStr html string that contains all the rows for a particular product.
    def _bldHtmlTabHdr(self):    
        msg = [] 
        
        msg.append('''<form><table id="sorttab1" class="tabsort">''')
        msg.append('<thead><tr id="hilite"><th>Environment</th><th>Server</th><th>OS</th><th>Version</th><th>Firewall</th><th>Domain</th>' \
                   '<th>Ver</th><th>HF</th><th>Node</th><th>Int Serv</th><th>Repo Serv</th><th>DB Name</th><th>DB Ver</th></tr></thead><tbody>')
        return ''.join(msg)
    
    def bldInfaPCTblHtml(self):
    
        html = []
        html.append(self._bldHtmlTabHdr())
        
        i=0     
        for s in self.scrd:     
            #mg.logger.debug("sname %s " % s)
            html.append(self._bldTR(i%2,s))
            i+=1    
        
        #html.append('<input type="hidden" name="cred_tab" value="%s">'  % self.__class__.__name__ )            
        html.append('</tbody></table></form><br>')
            
        return ''.join(html)
    #This method builds rows based on env. Color changes 
    # rn  row number for displaying purposes.
    # sc  server credentials.
    def _bldTR(self,rn,sc):
        diag = '/softinv/si_admin/download/env/%s_%s.vsd' % (sc.env_bu, sc.env_name)
        return ('<tr class="d%d"><td><a href="#" onclick="javascript:popup_geo(\'%s\',5,5);"> %s&nbsp; %s </a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>' \
                '<td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td</tr>'  % (       rn,
                                                                                         diag,sc.env_bu, sc.env_name, 
                                                                                         sc.serv_name ,
                                                                                         sc.serv_os   ,  
                                                                                         sc.serv_ver  , 
                                                                                         'No' if ( sc.auth != 1 ) else 'Yes',
                                                                                         ' '  if ( sc.dom_name is None) else  sc.dom_name,
                                                                                         sc.dom_ver  ,
                                                                                         sc.dom_hf   ,
                                                                                         sc.node_name,
                                                                                         sc.int_name ,
                                                                                         ' '  if ( sc.repo_name is None) else sc.repo_name,
                                                                                         ' '  if ( sc.db_ident is None)  else sc.db_ident ,
                                                                                         ' '  if ( sc.db_ver   is None)  else sc.db_ver   )        
                   )


# Class for PWX. Need to be implemented.
class InfaPWXHTML(RenderHTML):

    def __init__(self,imgDir):
        self.imgDir = imgDir
        self.scrd   = mg.infa_pwx_rs

    def bldInfaPWXTblHtml(self):
        return "IN Construction ..."    

# Software Vendors
class SoftVendHTML(RenderHTML):
        
    def __init__(self,imgDir):
        self.imgDir = imgDir
 
    def bldHtmlHeader(self):
        strList = []
        strList.append('<html><head><style type="text/css">')
        strList.append('h2{text-align: center}')
        strList.append('tr.d0 td {background-color:#F0F0F0; color: black;} tr.d1 td {background-color:#DCDCDC; color: black;}')
        strList.append('table.envdisp {border-width: 1px; border-style: outset;border-color: gray;border-collapse: collapse;margin-left: 10%;width: 40%;}')
        #strList.append('table.envdisp th { border-width: 1px;border-color: gray; text-align: left;}')
        #strList.append('table.envdisp td { border-width: 1px;border-style: inset;border-color: gray;}')
        strList.append('#hilite { background-color: #99CCFF;font-weight:bold;color: blue;}')
        strList.append('</style></head><body>')
        return ''.join(strList)
    
    
    # Build static part of the vendor.
    def _bldHtmlTabHdr(self,vend):    
        msg = [] 
        msg.append('<h3>%s</h3><table class="envdisp"><tr id="hilite"><tr bgcolor="#AFEEEE" align="left">' % vend.name )
        msg.append('<tr align="left"> <td> <a href="%s" </a> %s</td></tr>' % (vend.url,vend.url))
        msg.append('<tr><td> %s<td></tr><tr><td> %s<td></tr><tr><td> %s<td></tr>' % (vend.addr1,vend.addr2,vend.zip))
                                                                                                  
                                                                                                   
        msg.append('<tr><td>Contact</td></tr>')
        return ''.join(msg)
    
    #This method builds rows based on env. Color changes 
    def _bldTR(self,rn,cont):
        return ('<tr><td>%s&nbsp;%s</td>' % ( cont.first_name, cont.last_name)                                                       
                )        
     
    def bldSoftVendHtml(self):
        html = []
        vrs = mg.vendor_rs  
        vp  = mg.vend_cont_pid
        print "VRS = " , vrs
        print "VP  = " , vp 
        try:    
            for v in vrs:
                i=0
                #mg.logger.debug("prod = %s" % (v))
                html.append(self._bldHtmlTabHdr(v))
                cont = vp[v.id]
                #print "XCONT " , cont
                #if len(cont) < 1 : continue
                for c in cont :
                    html.append(self._bldTR(i%2,c))
                    i+=1
                html.append('</table><br>')
            
            return ''.join(html)
             
        except:
            mg.logger.debug('EXCEP Vendor = %s  Vendor ID %s exception %s' % (v.name, v.id,sys.exc_info()[0]))
            return ''
        
     
    def renderSoftVend(self):
        return self.bldSoftVendHtml()
    
    def renderSoftVendStdAlone(self):    
        html = []
        html.append(self.bldHtmlHeader() )  
        html.append(self.bldSoftVendHtml())
        html.append(self.bldHtmlFooter() )
        return ''.join(html)
    

# Render downloads table.
# Need to submit table only.
class SoftVerHTML(RenderHTML):

    def __init__(self,imgDir):
        self.imgDir = imgDir
     
    def _bldHtmlTabHdr(self,product):    
        msg = [] 
        msg.append('<h3>%s</h3><table class="contdisp">' % product )
        msg.append('<colgroup><col id="c1"><col id="c1"> <col id="c1"><col id="c1"><col id="c4"><col id="c4"><col id="c4"><col id="c8"></colgroup>')
        msg.append('<tr id="hilite"><tr bgcolor="#AFEEEE" align="left">' )
        msg.append('<tr bgcolor="#AFEEEE" align="left"><th>Version</th><th>Patch</th><th>Binary</th><th>Date</th>' \
                   '<th>Download Email</th><th>Support Matrix OS</th><th>Support Matrix DB</th><th>&nbsp;Download Directory Path</th></tr>')     
        return ''.join(msg)

    # Oracle returns date as '2006-10-30 00:00:00', need to convert to 2006-10-30.
    # For DB portability do not truncate date at the DB Level (SQL Query).
    # prod product to include in downloa dpath.
    def _bldTR(self,rn,ver):
        email = '/softinv/si_admin/download/email/%s'  % (ver.down_email     )
        sm_os = '/softinv/si_admin/download/matrix/%s' % (ver.supp_matrix_os )
        sm_db = '/softinv/si_admin/download/matrix/%s' % (ver.supp_matrix_db )

        return ('<tr class="d%d"> <td>%s</td> <td>%s</td> <td>%s</td> <td>%s</td>' \
                '<td><a href="#" onclick="javascript:popup_geo(\'%s\',5,5);"> %s </a></td>' \
                '<td><a href="#" onclick="javascript:popup_geo(\'%s\',5,5);"> %s </a></td>' \
                '<td><a href="#" onclick="javascript:popup_geo(\'%s\',5,5);"> %s </a></td>' \
                '<td><a id="sn" title="%s"  href="%s/%s">$SOFT_DEPO\\</a>%s</td></tr>' % (rn,
                                                                  ver.version     ,     
                                                                  ver.patch       ,  
                                                                  ver.bin         ,     
                                                         ver.down_date if (type(ver.down_date) == type('str') ) else ver.down_date.strftime("%m/%d/%Y"),   
                                                                  email,ver.down_email,     
                                                                  sm_os,ver.supp_matrix_os, 
                                                                  sm_db,ver.supp_matrix_db, 
                                                                  mg.softdepo, mg.softdepo,ver.dir_path,ver.dir_path 
                                                                   )
                )
        
    def bldSoftVerHtml(self):
        html = []
#        vrs = mg.vendor_rs 
#        sv  = mg.soft_ver_pid
#        for v in vrs:
#            if sv.has_key(v.id):
#                
#                for prod in sv[v.id]:         # prod is a dictionary {3:[  ]} contains a list of softver beans
#                    #                                                             {1:[  ]}
#                    for pid in prod.keys():                # pid is the key for prod.
#                        i = 0 
#                        #mg.logger.debug('prodname = %s\tpid = %s' % (pid,psi.getProdName(pid,mg.prod_rs )) )
#                        html.append(self._bldHtmlTabHdr('%s - %s ' % ( v.name,psi.getProdName(pid,mg.prod_rs )) ) )
#                        for e in prod[pid]:                        # Get associates bean for prod[pid)  
#                            html.append(self._bldTR(i%2,e))
#                            i+=1
#                        html.append('</table><br>')
        return ''.join(html)
    
    def renderSoftVer(self):
        return self.bldSoftVerHtml()
    
    def renderSoftVerStdAlone(self):   
        html = []
        html.append(self.bldHtmlHeader() )  
        html.append(self.bldSoftVerHtml())
        html.append(self.bldHtmlFooter() )
        return ''.join(html)


# Render Main Vendor Page. All Vendors, All downloads.
# Need to submit table only.
# End of life support
class SoftEOLHTML(RenderHTML):

    def __init__(self,imgDir):
        self.imgDir = imgDir
     
    def _bldHtmlTabHdr(self,product):    
        msg = [] 
        msg.append('<h3>%s</h3><table class="contdisp">' % product )
        #msg.append('<colgroup><col id="c1"><col id="c1"> <col id="c1"><col id="c1"><col id="c4"><col id="c4"><col id="c4"><col id="c8"></colgroup>')
        msg.append('<tr id="hilite"><tr bgcolor="#AFEEEE" align="left">' )
        msg.append('<tr bgcolor="#AFEEEE" align="left"><th>Version</th><th>Release Date</th><th>End Standard Supp</th><th>End Extended Supp</th>')
        return ''.join(msg)

    def _bldTR(self,rn,eol):
    
        return ('<tr class="d%d"> <td>%s</td> <td>%s</td> <td>%s</td> <td>%s</td>' % (  rn,
                                                                  eol.version     ,     
                                                                  eol.rel_date    ,          
                                                                  eol.eostd_supp_date,        
                                                                  eol.eoext_supp_date, 
                                                                   )
                )
        
    def bldSoftEOLHtml(self):
        html = []
#        vrs = mg.vendor_rs 
#        sv  = mg.soft_eol_pid
#        for v in vrs:
#            if sv.has_key(v.id):
#                
#                for prod in sv[v.id]:        # prod is a dictionary {3:[  ]} contains a list of softver beans
#                    #                                                             {1:[  ]}
#                    for pid in prod.keys():                # product id (pid) is the key for prod.
#                        i = 0 
#                        #mg.logger.debug('prodname = %s\tpid = %s' % (pid,psi.getProdName(pid,mg.prod_rs )) )
#                        html.append(self._bldHtmlTabHdr('%s - %s ' % ( v.name,psi.getProdName(pid,mg.prod_rs )) ) )
#                        for e in prod[pid]:                        # Get associates bean for prod[pid)  
#                            html.append(self._bldTR(i%2,e))
#                            i+=1
#                        html.append('</table><br>')
        return ''.join(html)
    
    def renderSoftEOL(self):
        return self.bldSoftEOLHtml()
    
    def renderSoftEOLStdAlone(self):   
        html = []
        html.append(self.bldHtmlHeader() )  
        html.append(self.bldSoftEOLHtml())
        html.append(self.bldHtmlFooter() )
        return ''.join(html)


# This class renders environment data for a specific vendor. 
class SoftInvHTML(RenderHTML):
      
    # imgDir : Image directory
    # vendor : Vendor Name e.g Informatica
    
    def __init__(self,imgDir,vendor):
        self.imgDir = imgDir
        self.vendor = vendor 
     
    # product name e.g. PowerCenter, PowerExchange.
    # htmlStr html string that contains all the rows for a particular product.
    def _bldHtmlTabHdr(self,product):    
        msg = [] 
       
       
        msg.append('''<h3>%s</h3><form><table id="sorttab1" class="tabsort" >''' % product)
        #msg.append('<h3>%s</h3><table class="envdisp"><tr id="hilite"><tr bgcolor="#AFEEEE" align="left">' % product )
        msg.append('<thead><tr id="hilite"><th>Env</th><th>BU</th><th>Server</th><th>Alias</th><th>OS</th><th>OS Ver</th>' \
                   '<th>OS Patch</th><th>SW Bin</th><th>SW Ver</th><th>SW Patch</th><th>SW License Path</th></tr></thead><tbody>')     
        return ''.join(msg)
    
    #This method builds rows based on env. Color changes 
    def _bldTR(self,rn,env):
        return ('<tr class="d%d"><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>' \
                '   <td>%s</td><td>%s</td><td>%s</td><td>%s</td> </tr>' % ( rn,
                                                                            env.envname   ,   
                                                                            env.bu        , 
                                                                            env.servname  , 
                                                                            env.alias     , 
                                                                            env.os        , 
                                                                            env.os_ver    , 
                                                                            env.os_patch  , 
                                                                            env.binary    , 
                                                                            env.ver       , 
                                                                            env.patch     , 
                                                                            env.lic_path , )
                )        
           
    def bldSoftInvHtml(self):
        html = []
#        id  = psi.getVendorId(self.vendor,mg.vendor_rs)
#        ep  = mg.env_pid
#        mg.logger.info('Vendor Name = %s Id = %d ' % (self.vendor,id))
#        
#        try:
#            vlst =  mg.vend_pid[id]
#            for pid in vlst:
#                i=0
#                mg.logger.debug("prod_id = %s  prod name = %s " % (pid.id,pid.name))
#                html.append(self._bldHtmlTabHdr(pid.name))
#                env = ep[pid.id]
#                #print str
#                for e in env :
#                    html.append(self._bldTR(i%2,e))
#                    i+=1
#                html.append('</tbody></table></form><br>')
#                #html.append('</table><br>')
#            
#            return ''.join(html)
#             
#        except:
#            mg.logger.debug('EXCEP Vendor = %s Id = %s Excep %s ' % (self.vendor,id,sys.exc_info()[0]))
#            return ''

    def renderSoftInv(self):
        return self.bldSoftInvHtml()
    
    def renderSoftInvStdAlone(self):    
        html = []
        html.append(self.bldHtmlHeader() )  
        html.append(self.bldSoftInvHtml())
        html.append(self.bldHtmlFooter() )
        return ''.join(html)

# Credentials.
# Parent Class renders software credentials 
class CredHTML(RenderHTML):
      
    # imgDir : Image directory
    # vendor : Vendor Name e.g Informatica
    
    def __init__(self,imgDir):
        self.imgDir = imgDir    

                                                                                                          
    def bldCredTblHtml(self,url=None):
    
        html = []
        html.append(self._bldHtmlTabHdr())
        
        i=0     
        for s in self.scrd:     
            #mg.logger.debug("sname %s " % s)
            html.append(self._bldTR(i%2,s,url))
            i+=1    
        
        #html.append('<input type="hidden" name="cred_tab" value="%s">'  % self.__class__.__name__ )            
        html.append('</tbody></table></form><br>')
            
        return ''.join(html)
             
#    def bldCredTblHtml(self): 
#        try:
#           i=0
#           for s in self.scrd:       
#                mg.logger.debug("sname %s " % s)
#                html.append(self._bldTR(i%2,s))
#                i+=1    
#                    
#            html.append('</table><br>')
#            
#            return ''.join(html)
#             
#        except:
#            mg.logger.debug('EXCEP server bean %s Excep %s ' % (s,sys.exc_info()[0]))
#            return ''

# Use for standalone html, not for rendering to web page.
    def renderStdAloneCred(self):
        html = []
        html.append(self.bldHtmlHeader() )  
        html.append(self.bldCredTblHtml())
        html.append(self.bldHtmlFooter() )
        return ''.join(html)
         
# This class renders Server Credentials 
class ServCredHTML(CredHTML):
      
    # imgDir : Image directory
    # vendor : Vendor Name e.g Informatica
    
    def __init__(self,imgDir):
        self.imgDir = imgDir
        self.scrd   = mg.serv_cred_rs
     
    # product name e.g. PowerCenter, PowerExchange.
    # htmlStr html string that contains all the rows for a particular product.
    def _bldHtmlTabHdr(self):    
        msg = [] 
        
        msg.append('''<form><table id="sorttab1" class="tabsort" onMouseOver="javascript:trackTableHighlight(event, '#8888FF');"  onMouseOut="javascript:highlightTableRow(0);"  >''')
        msg.append('<thead><tr id="hilite"><th>BU</th><th>Env</th><th>Server</th><th>OS</th><th>Firewall</th><th>user</th><th class="pwd">password</th></tr></thead><tbody>')
        return ''.join(msg)
    
    #This method builds rows based on env. Color changes 
    # rn  row number for displaying purposes.
    # sc  server credentials.
    def _bldTR(self,rn,sc,url):
        if sc.auth is None: auth = 'No'
        else              : auth = '<a href="%s" </a> Firewall </td>' % sc.auth    
        
        oc = """ onclick="popup_uc('%s/%s/%s/%s/%s');" """ % (url,sc.serv_id,sc.sname, sc.uname,'SERVER_CRED')
        return ('<tr class="d%d" %s ><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td class="pwd">%s</td></tr>'  % ( rn, oc,
                                                                                                               sc.env_bu   , 
                                                                                                               sc.env_name ,   
                                                                                                               sc.sname    , 
                                                                                                               sc.os       , 
                                                                                                               auth        ,
                                                                                                               sc.uname    , 
                                                                                                               sc.pwd      ,)        
                   )
                                                                                                       


# This class renders DB Server Credentials 
class DBCredHTML(CredHTML):
      
    # imgDir : Image directory
    # vendor : Vendor Name e.g Informatica
    
    def __init__(self,imgDir):
        self.imgDir = imgDir
        self.scrd   = mg.db_cred_rs
     
    # product name e.g. PowerCenter, PowerExchange.
    # htmlStr html string that contains all the rows for a particular product.
    # bgcolor="#AFEEEE"
    def _bldHtmlTabHdr(self):    
        msg = [] 
        msg.append('''<form><table id="sorttab2" class="tabsort" onMouseOver="javascript:trackTableHighlight(event, '#8888FF');"  onMouseOut="javascript:highlightTableRow(0);"  >''')
        msg.append('<thead><tr id="hilite"><th>DB Service</th><th>DB Host</th><th>Firewall</th><th>user</th><th class="pwd">password</th></tr></thead><tbody>')
        return ''.join(msg)
    
    #This method builds rows based on env. Color changes 
    # rn  row number for displaying purposes.
    # sc  server credentials.
    def _bldTR(self,rn,sc,url):
        if sc.auth is None: auth = 'No'
        else              : auth = '<a href="%s" </a> Firewall </td>' % sc.auth   
        oc = """ onclick="popup_uc('%s/%s/%s/%s/%s');" """ % (url,sc.dbrepo_id,sc.sname,sc.uname,'DB_CRED')
        return ('<tr class="d%d" %s ><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td class="pwd">%s</td></tr>'  % ( rn,oc,
                                                                                                    sc.sname    , 
                                                                                                    sc.db_host  ,
                                                                                                    auth        ,
                                                                                                    sc.uname    , 
                                                                                                    sc.pwd      ,)        
                )
                        
# This class renders Informatica Domain Credentials 
class InfaDomCredHTML(CredHTML):
      
    # imgDir : Image directory
    # vendor : Vendor Name e.g Informatica
    
    def __init__(self,imgDir):
        self.imgDir = imgDir
        self.scrd   = mg.dom_cred_rs
     
    # product name e.g. PowerCenter, PowerExchange.
    # htmlStr html string that contains all the rows for a particular product.
    def _bldHtmlTabHdr(self):    
        msg = [] 
        msg.append('''<form><table id="sorttab3" class="tabsort" onMouseOver="javascript:trackTableHighlight(event, '#8888FF');"  onMouseOut="javascript:highlightTableRow(0);"  >''')
        msg.append('<thead><tr id="hilite"><th>BU</th><th>Env</th><th>Domain URL</th><th>Dom Name</th><th>host</th><th>Firewall</th><th>user</th><th class="pwd">password</th></tr></thead><tbody>')
        return ''.join(msg)
    
    #This method builds rows based on env. Color changes 
    # rn  row number for displaying purposes.
    # sc  server credentials.
    def _bldTR(self,rn,sc,url):
        if sc.auth is None: auth = 'No'
        else              : auth = '<a href="%s" </a> Firewall </td>' % sc.auth
        u  = '%s:%s' % (sc.serv_name,sc.dom_port)
        oc = """ onclick="popup_uc('%s/%s/%s/%s/%s');" """ % (url,sc.dom_id,sc.sname,sc.uname,'INFA_DOM_CRED')
        return ('<tr class="d%d" %s><td>%s</td><td>%s</td><td><a href="http://%s" </a>http://%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td class="pwd">%s</td></tr>'  % ( rn,oc,
                                                                                                               sc.env_bu    , 
                                                                                                               sc.env_name  ,  
                                                                                                               u,u, 
                                                                                                               sc.sname     ,
                                                                                                               sc.serv_name , 
                                                                                                               auth,
                                                                                                               sc.uname     , 
                                                                                                               sc.pwd       ,)        
                )
                        

# This class renders Informatica Domain Credentials 
class InfaRepoCredHTML(CredHTML):
      
    # imgDir : Image directory
    # vendor : Vendor Name e.g Informatica
    
    def __init__(self,imgDir):
        self.imgDir = imgDir
        self.scrd   =  mg.repo_cred_rs 
        #print self.scrd
#        for b in self.scrd:
#            print b 
     
    # product name e.g. PowerCenter, PowerExchange.
    # htmlStr html string that contains all the rows for a particular product.
    def _bldHtmlTabHdr(self):    
        msg = [] 
        msg.append('''<form><table id="sorttab4" class="tabsort" onMouseOver="javascript:trackTableHighlight(event, '#8888FF');"  onMouseOut="javascript:highlightTableRow(0);"  >''')
        msg.append('<thead><tr id="hilite"><th>BU</th><th>Env</th><th>Repo Name</th><th>DB Instance</th><th>host</th><th>Firewall</th><th>user</th><th class="pwd">password</th></tr></thead><tbody>')
        return ''.join(msg)
    
    #This method builds rows based on env. Color changes 
    # rn  row number for displaying purposes.
    # sc  server credentials.
    def _bldTR(self,rn,sc,url):
        if sc.auth is None: auth = 'No'
        else              : auth = '<a href="%s" </a> Firewall </td>' % sc.auth   
        oc = """ onclick="popup_uc('%s/%s/%s/%s/%s');" """ % (url,sc.repo_id,sc.sname,sc.uname,'INFA_REPO_CRED')
        return ('<tr class="d%d" %s><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td class="pwd">%s</td></tr>'  % ( rn,oc,
                                                                                                               sc.env_bu    , 
                                                                                                               sc.env_name  ,                                                                              
                                                                                                               sc.sname     ,
                                                                                                               sc.db_name   ,
                                                                                                               sc.serv_name , 
                                                                                                               auth         ,
                                                                                                               sc.uname     , 
                                                                                                               sc.pwd       ,)        
               )
        return '      '

# Operations 
# Uptime server statistucs
class InfaServUpTimeHTML(RenderHTML):
    def __init__(self,imgDir,upt):
        self.imgDir = imgDir
        self.upt    = upt

    
    def bldServUpTimeTblHtml(self):
    
        html = []
        html.append(self._bldHtmlTabHdr())
        
        i=0     
        for u,s in self.upt:     
            #mg.logger.debug("sname %s " % s)
            html.append(self._bldTR(i%2,u))
            i+=1    
        
        #html.append('<input type="hidden" name="cred_tab" value="%s">'  % self.__class__.__name__ )            
        html.append('</tbody></table></form><br>')
            
        return ''.join(html)
    
    def _bldHtmlTabHdr(self):    
        msg = [] 
        
        msg.append('''<form><table id="sorttab1" class="tabsort">''')
        msg.append('<thead><tr id="hilite"><th>Environment</th><th>Server</th><th>Port</th><th>Domain </th><th>Start Time</th><th>Uptime</th></tr></thead><tbody>')
        return ''.join(msg)


    # This method builds rows based on env. Color changes 
    # rn  row number for displaying purposes.
    # ut  response 
    def _bldTR(self,rn,ut):
        return ('<tr class="d%d"><td>%s %s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>')  % (rn,
                                                                                          ut.bu, ut.env_name, 
                                                                                          ut.serv_name ,
                                                                                          ut.dom_port  ,  
                                                                                          ut.dom_name  ,
                                                                                          ut.st,
                                                                                          ut.ut )


    def renderStdAloneServUp(self):
        html = []
        html.append(self.bldHtmlHeader() )  
        html.append(self.bldServUpTimeTblHtml())
        html.append(self.bldHtmlFooter() )
        return ''.join(html)
         

# This class renders Server Credentials 
class ServRloginHTML(RenderHTML):    
    # imgDir : Image directory
    # vendor : Vendor Name e.g Informatica
    
    def __init__(self,imgDir,rl):
        self.imgDir    = imgDir
        self.rloginLnk = rl
        self.scrd      = mg.serv_cred_rs
     
    def bldRloginTblHtml(self):
    
        html = []
        html.append(self._bldHtmlTabHdr())
        
        i=0     
        for s in self.scrd:     
            #mg.logger.debug("sname %s " % s)
            html.append(self._bldTR(i%2,s))
            i+=1    
        
        #html.append('<input type="hidden" name="cred_tab" value="%s">'  % self.__class__.__name__ )            
        html.append('</tbody></table></form><br>')
            
        return ''.join(html)
    
    def _bldHtmlTabHdr(self):    
        msg = [] 
        
        msg.append('''<form><table id="sorttab1" class="tabsort">''')
        msg.append('<thead><tr id="hilite"><th>Environment</th><th>Server</th><th>User</th><th>OS</th><th>Status</th><th>Action</th></tr></thead><tbody>')
        return ''.join(msg)


    # This method builds rows based on env. Color changes 
    # rn  row number for displaying purposes.
    # ut  response 
    # status 1 means that is ENABLED and you can rlogin.
    def _bldTR(self,rn,sb):
        if (sb.status == 1 and sb.os != 'WIN'):
            if sb.uname is not None:
                btn = '<input type="button" value="Disable" onclick="location.href=\'updCredStat/server/%s/%s/0\'"/>' % (sb.serv_id,sb.uname)
                img = '<img src="%s/enable.gif">' % self.imgDir 
            else :
                btn = 'N/A'
                img = '<img src="%s/skip.gif">' % self.imgDir
                
            tr = '<tr class="d%d"><td>%s %s</td><td>%s</td> <td>%s</td><td>%s</td><td>%s</td><td>%s</td>'  % (rn,
                                                                                          sb.env_bu, sb.env_name, 
                                                                                          '<a href="%s/%s/%s" </a> %s' % ( self.rloginLnk, sb.serv_id , sb.uname, sb.sname),
                                                                                          sb.uname ,  
                                                                                          sb.os,
                                                                                          img,
                                                                                          btn)
                                
   
        else:
            if sb.os != 'WIN' and sb.uname is not None:  
                btn =  '<input type="button" value="Enable" onclick="location.href=\'updCredStat/server/%s/%s/1\'"/>' % (sb.serv_id,sb.uname)     
                img = '<img src="%s/disable.gif">' % self.imgDir
            else              : 
                btn = 'N/A'
                img = '<img src="%s/skip.gif">' % self.imgDir
            
            tr = '<tr class="d%d"><td>%s %s</td><td>%s</td> <td>%s</td><td>%s</td><td>%s</td><td>%s</td>'  % (rn,
                                                                                          sb.env_bu, sb.env_name, 
                                                                                          sb.sname ,
                                                                                          sb.uname ,  
                                                                                          sb.os,
                                                                                          img,
                                                                                          btn)        
        return tr





    def renderStdAloneServUp(self):
        html = []
        html.append(self.bldHtmlHeader() )  
        html.append(self.bldRloginTblHtml())
        html.append(self.bldHtmlFooter() )
        return ''.join(html)
         

# For thi class 
class CommonHTML(RenderHTML):  
    
    def __init__(self,imgDir):
        self.imgDir = imgDir
    
    def bldHtmlHeader(self):
        strList = []
        strList.append('<html><head><style type="text/css">')
        #strList.append('h2{text-align: center}')
        #strList.append('tr.d0 td {background-color:#F0F0F0; color: black;} tr.d1 td {background-color:#DCDCDC; color: black;}')
        strList.append('table.envdisp {border-width: 1px; border-style: outset;border-color: gray;border-collapse: collapse;margin-left: 10%;width: 100%;}')
        strList.append('table.envdisp th { border-width: 1px;border-color: gray; text-align: left;}')
        strList.append('table.envdisp td { border-width: 1px;border-style: inset;border-color: gray;}')
        strList.append('#hilite { background-color: #99CCFF;font-weight:bold;color: blue;}')
        strList.append('</style></head><body>')
        return ''.join(strList)
    

    def bldEndTagHtml(self)  : return '</body></html>'     
    
    # row is the header row.
    def _bldHtmlTabHdr(self,row):    
        msg = [] 
        msg.append('''<table id="envdisp" class="envdisp"> <thead><tr id="hilite">''')
        s= ''
        for r in row:
            s += '<th>%s</th> ' % r
        msg.append('%s </tr></thead><tbody>' % s)
        return ''.join(msg)   

    def _bldTR(self,data):
        
        s = ''
        for row in data:
            s += '<tr>'
            for r in row:
                s += '<td>%s</td> ' % r
            s += '</tr>'
        s+= '</tbody></table>'    
        return s 
    
    def bldHTMLTab(self,data,hdr=True):
        htmls = ''
        dl    =  len(data)
        
        if hdr and dl > 1 : 
                htmls  = self._bldHtmlTabHdr(data[0])
                htmls += self._bldTR(data[1:])
        
        elif dl > 0:
                htmls = self._bldTR(data)
        
        return htmls
           
# Remote logins (ssh)
# EO For testing only   
def interrogate(item):
    """Print useful information about item."""
    if hasattr(item, '__name__'):
        print "NAME:    ", item.__name__
    if hasattr(item, '__class__'):
        print "CLASS:   ", item.__class__.__name__
    print "ID:      ", id(item)
    print "TYPE:    ", type(item)
    print "VALUE:   ", repr(item)
    print "CALLABLE:",
    if callable(item):
        print "Yes"
    else:
        print "No"
        
        
def test_common():
    
    data = [
    ("source    ",   "total_cost" ,      "total_records"),
    ("EI     ",    393.98      ,     23            ),
    ("FL     ",    28.39      ,     3            ),
    ("PO     ",    12321.96      ,     58            ),
    ("RC     ",    364.0      ,     7            ),
    ("RO     ",    173711.84      ,     2284        ),
    ("WA     ",    5424.86      ,     32            ),
    ("~TOTAL~",  192245.03      ,     2407                   ),   ]    
        
   
    o = CommonHTML(' ')
    html = []
    html.append(o.bldHtmlHeader() )
    html.append(o.bldHTMLTab(data))
    html.append(o.bldEndTagHtml() )  
    print 'HTML: = %s' % ''.join(html)
    
def bld_test_hmtl(o):
        html = []
        html.append(o.bldHtmlHeader() )  
        html.append(o.bldTblHtml())
        html.append(o.bldHtmlFooter() )
        return ''.join(html)
 
if __name__ == "__main__":
    #from maindriver import startup, shutdown,getCredData # EO For unit testing ONLY ! Comment in prod

    cwd = 'C:\infaapp\softinv'
    
    #startup(cwd)
    # Populate data First from different modules.Do not cimment this lines
    #psi.getSoftInvData()
    
    # Soft Inv
    
    
    # TEST SI vendor specific
    
    #si = SoftInvHTML('a','Informatica')
    #si = SoftInventoryHTML('a','Computer Associates')
    #print si.renderSoftInvStdAlone()
       
    #  All Vendors s
    #sv = SoftVendHTML('a')
    #print sv.renderSoftVendStdAlone()
 
    # Versions
    #sv =  SoftVerHTML('a')
    #print sv.renderSoftVerStdAlone()
    #Test download info page 
    
    #EOL 
    #eol = SoftEOLHTML('a')
    #print eol.renderSoftEOLStdAlone()
   
    # main PC page
    #m = InfaPCHTML('a')
    #print m.bldInfaPCTblHtml()
    

    #print bld_test_hmtl(im)
    # Credentials 
    #url = '/softinv/si_main/main'
    #ti = ServCredHTML('a')
    #ti = DBCredHTML('a')
    #interrogate(ti)
    #print ti.renderStdAloneCred()
   
    #ti = InfaDomCredHTML('a')
#    ti  = InfaRepoCredHTML('a')
    
    #Operations
#    sl=ServRloginHTML(r'C:\infaapp\static\images','/softinv/si_admin/servlogin') 
#    print sl.renderStdAloneServUp()
    test_common()
    # shutdown()