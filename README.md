# FundAdmin
Scripts (Python, Shell, Perl e SQL) que acumulam dados de fundos de investimentos da CVM bem como valores diários de CDI, IPCA e outros indexadores do mercado brasileiro.

O script cvmweb.py é o que faz toda a mágica da coleta e deve ser rodado diariamente.
Os arquivos MWB contém o design de tabelas produzido pelo MySQL WorkBench.

O banco de dados resultante contém dados diários de fundos de investimentos do Brasil.
O layout dos dados contém também tabelas para um usuário registrar aportes e saques que fez em datas específicas em certos fundos.
As views em fundadmin-views.sql são as que entregam insights personalizados (saldo e performance da carteira do usuário) sobre os dados coletados da CETIP e CVM cruzados com a carteira do usuário.

Eu desenvolvi isso em 2013/2014 e rodei o script diariamente por alguns anos.
A quem se interessar, eu tenho proximadamente 1GB de dados de fundos de investimentos brasileiros que posso passar como um arquivo SQLite.
