'''
Created on Nov 5, 2010
@author: emo0uzx


'''



#===============================================================================
# Standalone auxiliary  methods.
#===============================================================================
#Data
#prod_rs       = []
#env_rs        = []    
#vendor_rs     = []
#vend_cont_rs  = []
#soft_ver_rs   = []
#
#env_pid       = {}      # Contains env in which key is prod_id.
#vend_pid      = {}      # Contains prod in which key is vend_id
#vend_cont_pid = {}      # Contains vend_contc in which key is vend_id
#soft_ver_pid  = {}      # Contains in which key is vend_id

def getVendorId (vname,v_rs):
    for vend in v_rs:
        if vend.name == vname: 
            return vend.id
    return -1        

def getVendorName(vid,v_rs):
    for vend in v_rs:
        if vend.id == vid:
            return vend.name
    return ''

def getProdName(pid,p_rs):
    for prod in p_rs:
        if prod.id == pid:
            return prod.name
    return ''

# This function gets the prod id's from prod beans.
# then creates an array w/all env beans that have the prod_id
# returns a dict { prod_id: env_array}
def setEnvProdId(p_rs,e_rs):
    d = {}
    for prod in p_rs:
        lst = []
        for env in e_rs:
            if env.prod_id == prod.id:
                lst.append(env) 
        d[prod.id] = lst
    return d

# This function gets the vendor id's from vendor beans.
# then creates an array w/all prod beans that have the vendor_id
# returns a dict { vend_id: prod_array}
def setVendorProdId(p_rs,v_rs):
    d = {}
    for vend in v_rs:
        lst = []
        for prod in p_rs:
            if prod.vend_id == vend.id:
                lst.append(prod)
        d[vend.id] = lst
    return d
            
# This function gets the vendor id's from vendor beans.
# then creates an array w/all prod beans that have the vendor_id
# returns a dict { vend_id: prod_array}
def setVendContProdId(v_rs,v_cnt_rs):
    d = {}
    for vend in v_rs:
        lst = []
        for cont in v_cnt_rs:
            if cont.vend_id == vend.id:
                lst.append(cont)
        d[vend.id] = lst
    return d


# 1 -This function creates a list of all software products (sp) for a particular vendor.
# 2- Then gets each product within the vendor and adds it the list into a dictionary.
# vid: Vendor Id
# 1 dictionary with one key per vendor
#            dict = { 1: vend id 
# e.g vid =             [ 
#                          {1:{[1.1,1.2 ..1.n]},  pid = 1
#                          {2:[2.1,2.2 ..2.n]}    pid = 2
#                       ]
#                     2: vend id
#                       [ 
#                          {3:{[3.1,3.2 ..3.n]}   pid = 3
#                       ]
#                   }
#       
# soft_rs -> res to iterate. Could be soft versions or soft eol
def _getVendorSoft(vid,mg,soft_rs):
    d = {}
    lst = []
    for sp in soft_rs:
        if sp.vend_id == vid:
            lst.append(sp)
                
    # Products within a vendor
    dlst=[]
    for pid in mg.vend_pid[vid]:
        pl  = [] ; prd = {}
        for p in lst:
            #sig.logger.debug( "p.prod_id  %d  pid.id = %d " % (p.prod_id, pid.id))
            if p.prod_id  == pid.id:
                pl.append(p)

        if len(pl) > 0 : 
            #mg.logger.info( "Creating list for vend=%s  prod=%s len = %d " % (vid, pid.id,len(pl)))
            prd[pid.id]= pl
            dlst.append(prd)
        
    d[vid] = dlst
    return  d    
        
# This function gets the vendor id from vendor beans and creates a dict in which the
# key is the vendor_id and contents is another dict whic key per product
# e.g.
# {1:{1:[]},{2:[]}} eg vendor id =1 , prod_id =1 , [rpd_id = 2 for that vendor_id)
def setSoftVerProdId(mg):
    d = {}
    for vend in mg.vendor_rs:

        #print('Vendor = % s id = %d' % (vend.name,vend.id) )
        m = _getVendorSoft(vend.id,mg,mg.soft_ver_rs)
        #print('setSoftVerProdId  Dict is %s' % m)
        d.update(m)
    
    return d


# This function gets the vendor id from vendor beans and creates a dict in which the
# key is the vendor_id and contents is another dict whic key per product
# e.g.
# {1:{1:[]},{2:[]}} eg vendor id =1 , prod_id =1 , [rpd_id = 2 for that vendor_id)
def setSoftEOLProdId(mg):
    d = {}
    for vend in mg.vendor_rs:

        #print('Vendor = % s id = %d' % (vend.name,vend.id) )
        m = _getVendorSoft(vend.id,mg,mg.soft_eol_rs)
        #print('setSoftVerProdId  Dict is %s' % m)
        d.update(m)
    
    return d

  
def _printLst(lst):
    
    if(len(lst) > 0):
        for r in lst:
            print "Records ", r

               
if __name__ == '__main__':
    #import mainglob as mg
    import maindriver
    maindriver.startup('C:\infaapp\softinv')

    maindriver.shutdown()
    
#    mg.printGlob()
#    _printLst( mg.soft_ver_rs)
#    print mg.soft_ver_pid
  
    #print getVendorId('Informatica')
    #mg.printGlob()