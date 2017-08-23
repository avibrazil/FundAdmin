#!/usr/bin/python
# -*- coding: utf-8 -*-

from collections import defaultdict
from suds.client import Client
from xml.etree import cElementTree as ET
import logging
import os
import sqlite3
import suds
import zipfile
import tempfile
import datetime
import pytz
import sys
import urllib2
logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.client').setLevel(logging.CRITICAL)



class FundsDB:
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
    
    def __del__(self):
        self.conn.commit()
        self.conn.close()
    
    def getMarketIndexHistoryDateRange(self,indexName):
        self.cursor.execute("""SELECT min(date) AS begin, max(date) AS end FROM market_index where name='%s'""" % indexName)
        row=self.cursor.fetchone()
        datetuple=(datetime.datetime.fromtimestamp(row[0]).replace(tzinfo=pytz.utc,hour=12,minute=0,second=0),
            datetime.datetime.fromtimestamp(row[1]).replace(tzinfo=pytz.utc,hour=12,minute=0,second=0))
        return datetuple
    
    def updateMarketIndexItem(self,name,date,value):
        self.cursor.execute('''INSERT OR REPLACE INTO market_index (date,name,value)
                            VALUES (?,?,?)''',
                            (date.strftime('%s'),name,value))

    def getFundQuoteHistoryDateRange(self):
        self.cursor.execute('''SELECT min(date) AS begin, max(date) AS end FROM fund_history''')
        row=self.cursor.fetchone()
        datetuple=(datetime.datetime.fromtimestamp(row[0],pytz.utc),datetime.datetime.fromtimestamp(row[1],pytz.utc))
        return datetuple
    
    def getCVMWebUsers(self):
        self.cursor.execute('''SELECT * from cvmweb_users''')
        users = [row for row in self.cursor]
        return users
        
    def getFundQuoteByDate(self,cnpj,datestamp):
        # Convert the '2013-03-21'-like date to a full noon time at UTC timezone
        normalizedDate=datetime.datetime.strptime(datestamp,'%Y-%m-%d').replace(hour=12,tzinfo=pytz.utc)
        row=self.cursor.execute('''SELECT quote FROM fund_history WHERE date<=? LIMIT 1''',normalizedDate.strftime('%s'))
        return row[0]
        
    def updateFundHistoryItem(self,cnpj,datestamp,quote,patrimonio,patrimonio_total,captation,resgate,cotistas):
        # Convert the '2013-03-21'-like date to a full noon time at UTC timezone
        normalizedDate=datetime.datetime.strptime(datestamp,'%Y-%m-%d').replace(hour=12,tzinfo=pytz.utc)
        self.cursor.execute('''INSERT OR REPLACE INTO fund_history (
                                    fundID,
                                    cnpj,
                                    date,
                                    quote,
                                    patrimonio,
                                    patrimonio_total,
                                    captation,
                                    resgate,
                                    cotistas)
                                SELECT * FROM (
                                    SELECT fundID,
                                           :1,:2,:3,:4,:5,:6,:7,:8
                                    FROM fund
                                    WHERE cnpj=:1
                                  UNION
                                    SELECT NULL,
                                           :1,:2,:3,:4,:5,:6,:7,:8
                                ) ORDER BY fundID DESC LIMIT 1;''',
                            (cnpj,
                             normalizedDate.strftime('%s'),
                             quote,
                             patrimonio,
                             patrimonio_total,
                             captation,
                             resgate,
                             cotistas))
        
    def updateSingleFundData(self, date, cnpj, cnpj_admin, status, date_init, date_const, classe, date_init_class, exclusive, quote, tratamento_tributario, qualified_investors,
                             condom_type,
                             benchmark,
                             perf_tax,
                             name,
                             name_admin):
        # self.cursor.execute('''INSERT OR REPLACE INTO fund (updated,cnpj,
                             # cnpj_admin,
                             # status,
                             # date_init,
                             # date_const,
                             # class,
                             # date_init_class,
                             # exclusive,
                             # quote,
                             # tratamento_tributario,
                             # qualified_investors,
                             # condom_type,
                             # benchmark,
                             # perf_tax,
                             # name,
                             # name_admin)
                             # VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                            # (date.strftime('%Y-%m-%d'),cnpj,
                            # cnpj_admin,
                            # status,
                            # date_init,
                            # date_const,
                            # classe,
                            # date_init_class,
                            # exclusive,
                            # quote,
                            # tratamento_tributario,
                            # qualified_investors,
                            # condom_type,
                            # benchmark,
                            # perf_tax,
                            # name,
                            # name_admin))
                            
                            
        self.cursor.execute(
             '''INSERT OR REPLACE INTO fund (
                        fundid,
                        updated,
                        cnpj,
                        cnpj_admin,status,
                        date_init,
                        date_const,
                        class,
                        date_init_class,
                        exclusive,
                        quote,
                        tratamento_tributario,
                        qualified_investors,
                        condom_type,
                        benchmark,
                        perf_tax,
                        name,
                        name_admin)
                    SELECT * FROM (
                        SELECT fundid,
                               :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17
                        FROM fund
                        WHERE cnpj=:2
                      UNION
                        SELECT NULL,
                               :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17
                    ) ORDER BY fundid DESC LIMIT 1;''',
                            (date.strftime('%Y-%m-%d'),
                            cnpj,
                            cnpj_admin,
                            status,
                            date_init,
                            date_const,
                            classe,
                            date_init_class,
                            exclusive,
                            quote,
                            tratamento_tributario,
                            qualified_investors,
                            condom_type,
                            benchmark,
                            perf_tax,
                            name,
                            name_admin))                            
                            
        


class MarketIndexWebClient:
  """"Methods to get various market indexes as CDI, IBV, IFIX etc"""

  def updateCDIData(self,db):
    '''CDI index for the date YYYY-MM-DD is located at ftp://ftp.cetip.com.br/IndiceDI/YYYYMMDD.txt'''
      
    daterange=db.getMarketIndexHistoryDateRange('CDI')
    
    current=daterange[1]
    while 1:
        current+=datetime.timedelta(days=1)
        if current >= datetime.datetime.now().replace(hour=0,minute=0,second=0,tzinfo=pytz.utc):
            break

        try:
            cdiinfo=urllib2.urlopen('ftp://ftp.cetip.com.br/IndiceDI/%s.txt' % current.strftime('%Y%m%d'))
        except urllib2.URLError:
            continue
        
        cdi=float(cdiinfo.read(100))
        cdiinfo.close()
        cdi/=100
        db.updateMarketIndexItem('CDI',current,cdi)
        print "%s: %f" % (current, cdi)



class CVMWebClient:
    """Methods to access CVM WebServices (http://cvmweb.cvm.gov.br/swb/default.asp?sg_sistema=sws)"""
    # next function from http://stackoverflow.com/a/10077069
    def etree_to_dict(self, t):
        d = {t.tag: {} if t.attrib else None}
        children = list(t)
        if children:
            dd = defaultdict(list)
            for dc in map(self.etree_to_dict, children):
                for k, v in dc.iteritems():
                    dd[k].append(v)
            d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.iteritems()}}
        if t.attrib:
            d[t.tag].update(('@' + k, v) for k, v in t.attrib.iteritems())
        if t.text:
            text = t.text.strip()
            if children or t.attrib:
                if text:
                    d[t.tag]['#text'] = text
            else:
                d[t.tag] = text
        return d
    
    def __init__(self, appid, password):
        self.client = Client('http://sistemas.cvm.gov.br/webservices/Sistemas/SCW/CDocs/WsDownloadInfs.asmx?WSDL')
        
        head = self.client.factory.create('sessaoIdHeader')
        head.IdSessao = appid
        self.client.set_options(soapheaders=head)

        try:
            self.client.service.Login(appid, password)
            #    print "Resultado do Login:", result
        except suds.WebFault:
            print "Invalid CVMWeb login"
            return
        
        # print client.last_received()
        head.IdSessao = self.client.last_received().getChild("soap:Envelope").getChild("soap:Header").getChild("sessaoIdHeader").getChild("IdSessao").getText()
        head.Guid = self.client.last_received().getChild("soap:Envelope").getChild("soap:Header").getChild("sessaoIdHeader").getChild("Guid").getText()
        self.client.set_options(soapheaders=head)

    def updateFundsData(self, date, db):
        result = ""
        try:
            result = self.client.service.solicAutorizDownloadCadastro(date.strftime('%Y-%m-%d'), 'Analise de fundos')
        except suds.WebFault as e:
            print e.args[0]
            return

        if not result:
            return

        print "Updating Funds information for date: " + date.strftime('%Y-%m-%d')

        (f, dwnzip) = tempfile.mkstemp()
        os.close(f)
        dwnfiledir = tempfile.mkdtemp()
    
        # Using wget instead of urllib2 because it handles better the buggy CVM web server with its chunked responses
        os.system("wget -q -O '" + dwnzip + "' '" + result + "'")

        xmlfile = ""
        with zipfile.ZipFile(dwnzip, 'r') as myzip:
            xmlfile = myzip.namelist()
            myzip.extract(xmlfile[0], dwnfiledir)
            myzip.close()
            os.unlink(dwnzip)
        
        cadastrosdoc = ET.parse(dwnfiledir + "/" + xmlfile[0])
#        h = self.etree_to_dict(cadastrosdoc.find("CABECALHO"))
        cadastros = self.etree_to_dict(cadastrosdoc.find("PARTICIPANTES"))
     
    #    pprint(cadastros['PARTICIPANTES']['CADASTRO'][0])
     
        for c in cadastros['PARTICIPANTES']['CADASTRO']:
            cadastro = c.copy()
            cadastro['CNPJ'] = cadastro['CNPJ'].replace(".", "").replace("/", "").replace("-", "").zfill(14)
            cadastro['CNPJ_ADMINISTRADOR'] = cadastro['CNPJ_ADMINISTRADOR'].replace(".", "").replace("/", "").replace("-", "").zfill(14)
            for k, v in cadastro.iteritems():
                if (not v):
                    cadastro[k] = ""
                if (v == u'N\xe3o'):
                    cadastro[k] = 0
                if (v == "Sim"):
                    cadastro[k] = 1
            
            
            cadastro['TAXA_PERFORMANCE']=cadastro['TAXA_PERFORMANCE'].replace(",",".")

            if cadastro['TAXA_PERFORMANCE']=="":
                cadastro['TAXA_PERFORMANCE']=0
        
            db.updateSingleFundData(date,
                                cadastro['CNPJ'],
                                cadastro['CNPJ_ADMINISTRADOR'],
                                cadastro['SITUACAO'],
                                cadastro['DT_INICIO'],
                                cadastro['DT_CONSTITUICAO'],
                                cadastro['CLASSE'],
                                cadastro['DT_INICIO_CLASSE'],
                                cadastro['EXCLUSIVO'],
                                cadastro['COTAS'],
                                cadastro['TRATAMENTO_TRIBUTARIO'],
                                cadastro['INVESTIDORES_QUALIFICADOS'],
                                cadastro['FORMA_CONDOMINIO'],
                                cadastro['INDICADOR_DESEMPENHO'],
                                cadastro['TAXA_PERFORMANCE'],
                                cadastro['NOME'],
                                cadastro['NOME_ADMINISTRADOR'])
        
        os.unlink(dwnfiledir + "/" + xmlfile[0])
        os.removedirs(dwnfiledir)

    def updateQuoteData(self, date, db):
        result = ""
        try:
            result = self.client.service.solicAutorizDownloadArqComptc(209, date.strftime('%Y-%m-%d'), 'Analise de fundos')
        except suds.WebFault as e:
            return

        if not result:
            return

        print "Updating Funds Quote data for date: " + date.strftime('%Y-%m-%d')

        (f, dwnzip) = tempfile.mkstemp()
        os.close(f)
        dwnfiledir = tempfile.mkdtemp()

        # Using wget instead of urllib2 because it handles better the buggy CVM web server with its chunked responses
        os.system("wget -q -O '" + dwnzip + "' '" + result + "'")

        xmlfile = ""
        with zipfile.ZipFile(dwnzip, 'r') as myzip:
            xmlfile = myzip.namelist()
            myzip.extract(xmlfile[0], dwnfiledir)
            myzip.close()
            os.unlink(dwnzip)
        historydoc = ET.parse(dwnfiledir + "/" + xmlfile[0])
#        h = self.etree_to_dict(cadastrosdoc.find("CABECALHO"))
        history = self.etree_to_dict(historydoc.find("INFORMES"))
     
    #    pprint(cadastros['PARTICIPANTES']['CADASTRO'][0])
     
        for h in history['INFORMES']['INFORME_DIARIO']:
#        for h in history['INFORMES']:
            if (type(h)==str):
               # Data not ready on CVM side. Abort.
               break
            item = h.copy()
            item['CNPJ_FDO'] = item['CNPJ_FDO'].replace(".", "").replace("/", "").replace("-", "").zfill(14)
            
            for k, v in item.iteritems():
                if (not v):
                    item[k] = ""
                item[k]=v.replace(",", ".")
        
            db.updateFundHistoryItem(
                                     item['CNPJ_FDO'],
                                     item['DT_COMPTC'],
                                     item['VL_QUOTA'],
                                     item['VL_TOTAL'],
                                     item['PATRIM_LIQ'],
                                     item['CAPTC_DIA'],
                                     item['RESG_DIA'],
                                     item['NR_COTST']
            )
        
        os.unlink(dwnfiledir + "/" + xmlfile[0])
        os.removedirs(dwnfiledir)
        

def main(argv=None):
    if argv is None:
        argv = sys.argv
    
    db = FundsDB("brasilfunds.db")
    
    marketIndex=MarketIndexWebClient()
    marketIndex.updateCDIData(db)
    
    now=datetime.datetime.now(pytz.utc).replace(hour=12,minute=0,second=0,microsecond=0)

    cvmusers=db.getCVMWebUsers()
    
    cli = CVMWebClient(cvmusers[0][2], cvmusers[0][3])
    
    cli.updateFundsData(now-datetime.timedelta(days=1), db)
    
    for u in cvmusers:
        print "CVM user: %s - %s" % (u[2], u[3])
        cli = CVMWebClient(u[2], u[3])
        
        daterange=db.getFundQuoteHistoryDateRange()
        
        for d in range(1,11):
    #        if (daterange[1]+datetime.timedelta(days=d) > datetime.datetime.utcnow()):
    #            break
            cli.updateQuoteData(daterange[1]+datetime.timedelta(days=d), db)
            
        for d in range(1,11):
            cli.updateQuoteData(daterange[0]-datetime.timedelta(days=d), db)
            
    


if __name__ == "__main__":
    main()
