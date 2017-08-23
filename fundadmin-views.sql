-- -----------------------------------------------------
-- View `saldo_diario`
-- -----------------------------------------------------
DROP VIEW `saldo_diario`;
CREATE VIEW saldo_diario AS
SELECT afh.accountid AS accountid,
       fh.cnpj AS cnpj,
       fh.date AS date,
       max(afh.date) AS lastmove,
       sum(afh.quote) AS squote,
       fh.quote AS quote,
       sum(afh.quote)*fh.quote AS saldo
FROM fund_history AS fh,
     account_fund_history AS afh
WHERE afh.fundcnpj=fh.cnpj
  AND afh.date<=fh.date
GROUP BY fh.date, cnpj, accountid
ORDER BY fh.date;
;

-- -----------------------------------------------------
-- View `fund_rendimento_diario`
-- -----------------------------------------------------
DROP VIEW `fund_rendimento_diario`;
CREATE VIEW fund_rendimento_diario AS
SELECT fh.date AS date,
       fh.cnpj AS cnpj,
       fh.quote/fhm1.quote-1 AS rendimento_diario
FROM fund_history AS fh,
     fund_history AS fhm1
WHERE fh.cnpj=fhm1.cnpj
  AND fhm1.date=
    (SELECT date
     FROM fund_history
     WHERE date<fh.date
       AND cnpj=fh.cnpj
     ORDER BY date DESC LIMIT 1);
;

-- -----------------------------------------------------
-- View `mi_variacao_diaria`
-- -----------------------------------------------------
DROP VIEW `mi_variacao_diaria`;
CREATE VIEW mi_variacao_diaria AS
SELECT mi.date AS date,
       mi.name AS name,
       mi.value/mi2.value-1 AS variacao_diaria
FROM market_index AS mi,
     market_index AS mi2
WHERE mi.name=mi2.name
  AND mi2.date=
    (SELECT date
     FROM market_index
     WHERE date<mi.date
     ORDER BY date DESC LIMIT 1);
;

-- -----------------------------------------------------
-- View `fund_rendimento_mensal`
-- -----------------------------------------------------
DROP VIEW `fund_rendimento_mensal`;
CREATE VIEW fund_rendimento_mensal AS
SELECT fh.date AS date,
       (fh.date-fhm1.date)/(24*3600) AS delta,
       fh.cnpj AS cnpj,
       fh.quote/fhm1.quote-1 AS rendimento_mensal
FROM fund_history AS fh,
     fund_history AS fhm1
WHERE fhm1.cnpj=fh.cnpj
  AND fh.date=
    (SELECT date
     FROM fund_history
     WHERE cnpj=fh.cnpj
       AND date<strftime('%s',fh.date,'unixepoch','start of month','+1 month')
     ORDER BY date DESC LIMIT 1)
  AND fhm1.date=
    (SELECT date
     FROM fund_history
     WHERE cnpj=fh.cnpj
       AND date<strftime('%s',fh.date,'unixepoch','start of month')
     ORDER BY date DESC LIMIT 1);
;

-- -----------------------------------------------------
-- View `mi_variacao_mensal`
-- -----------------------------------------------------
DROP VIEW `mi_variacao_mensal`;
CREATE VIEW mi_variacao_mensal AS
SELECT mi.date AS date,
       (mi.date-mim1.date)/(24*3600) AS delta,
       mi.name AS name,
       mi.value/mim1.value-1 AS variacao_mensal
FROM market_index AS mi,
     market_index AS mim1
WHERE mim1.name=mi.name
  AND mi.date=
    (SELECT date
     FROM market_index
     WHERE name=mi.name
       AND date<strftime('%s',mi.date,'unixepoch','start of month','+1 month')
     ORDER BY date DESC LIMIT 1)
  AND mim1.date=
    (SELECT date
     FROM market_index
     WHERE name=mi.name
       AND date<strftime('%s',mi.date,'unixepoch','start of month')
     ORDER BY date DESC LIMIT 1);
;

-- -----------------------------------------------------
-- View `fund_perform_market_index_mensal`
-- -----------------------------------------------------
DROP VIEW `fund_perform_market_index_mensal`;
CREATE VIEW fund_perform_market_index_mensal AS
 SELECT frm.date AS date,
        rendimento_mensal,
        mivm.variacao_mensal AS index_var,
        rendimento_mensal/mivm.variacao_mensal AS performance,
        mivm.name AS index_name,
        fund.cnpj AS cnpj,
        fund.name AS name
 FROM fund_rendimento_mensal AS frm
 JOIN mi_variacao_mensal AS mivm
      ON strftime('%Y-%m',mivm.date,'unixepoch')=strftime('%Y-%m',frm.date,'unixepoch')
 JOIN fund
      ON fund.cnpj=frm.cnpj;
;

-- -----------------------------------------------------
-- View `fund_perform_market_index_diario`
-- -----------------------------------------------------
DROP VIEW `fund_perform_market_index_diario`;
CREATE VIEW fund_perform_market_index_diario AS
 SELECT frd.date AS date,
        rendimento_diario,
        mivd.variacao_diaria AS index_var,
        rendimento_diario/mivd.variacao_diaria AS performance,
        mivd.name AS index_name,
        fund.cnpj AS cnpj,
        fund.name AS name
 FROM fund_rendimento_diario AS frd
 JOIN mi_variacao_diaria AS mivd
      ON mivd.date=frd.date
 JOIN fund
      ON fund.cnpj=frd.cnpj;
;

-- -----------------------------------------------------
-- View `saldo_ultimo`
-- -----------------------------------------------------
DROP VIEW `saldo_ultimo`;
CREATE VIEW saldo_ultimo AS
SELECT afh.accountid AS accountid,
       fh.cnpj AS cnpj,
       date(fh.date,'unixepoch') AS date,
       date(max(afh.date),'unixepoch') AS lastmove,
       fund.class as class,
       sum(afh.quote) AS squote,
       fh.quote AS quote,
       100*(fh.quote/fhp.quote-1) as rendimento30,
       sum(afh.quote)*fh.quote AS saldo,
       fund.name as name
FROM fund_history AS fh,
     fund_history AS fhp,
     account_fund_history AS afh,
     fund
WHERE afh.fundcnpj=fh.cnpj
  AND fhp.cnpj=afh.fundcnpj
  AND afh.date<=fh.date
  AND fund.cnpj=fh.cnpj
  AND fh.date=(select max(date) from fund_history limit 1)
  AND fhp.date=(select date from fund_history where date<strftime('%s',fh.date,'unixepoch','-30 day') order by date desc limit 1)
GROUP BY fh.date, cnpj, accountid
ORDER BY accountid, class;
;

-- -----------------------------------------------------
-- View `saldo`
-- -----------------------------------------------------
DROP VIEW `saldo`;
CREATE VIEW saldo AS
SELECT *
FROM saldo_ultimo
WHERE squote>0.1
UNION
  SELECT accountid, '-',date,'-',
                             CLASS,
                             '-',
                             '-',
                             sum(saldo),
                             'TOTAL'
  FROM saldo_ultimo WHERE saldo>0.1 AND CLASS LIKE '%Multimercado%'
   GROUP BY accountid
UNION
  SELECT accountid, '-',date,'-',
                             CLASS,
                             '-',
                             '-',
                             sum(saldo),
                             'TOTAL'
  FROM saldo_ultimo WHERE saldo>0.1 AND CLASS LIKE '%Referen%'
   GROUP BY accountid
UNION
  SELECT accountid, '-',date,'-',
                             CLASS,
                             '-',
                             '-',
                             sum(saldo),
                             'TOTAL'
  FROM saldo_ultimo WHERE saldo>0.1 AND CLASS LIKE '%Ações%'
  GROUP BY accountid
UNION
  SELECT accountid, '-',date,'-',
                             CLASS,
                             '-',
                             '-',
                             sum(saldo),
                             'TOTAL'
  FROM saldo_ultimo WHERE saldo>0.1 AND CLASS LIKE '%Renda Fixa%'
  GROUP BY accountid
UNION
SELECT *
FROM
  ( SELECT accountid, '-', date, '-',
                                 'TOTAL',
                                 '-',
                                 '-',
                                 sum(saldo),
                                 'TOTAL'
   FROM saldo_ultimo
   GROUP BY accountid)
ORDER BY CLASS;
;

-- -----------------------------------------------------
-- View `account_fund_history_value`
-- -----------------------------------------------------
DROP VIEW `account_fund_history_value`;
CREATE VIEW account_fund_history_value AS
SELECT accountid,
       fundcnpj AS cnpj,
       afh.date AS date,
       afh.oper AS oper,
       afh.quote*fh.quote AS value
FROM account_fund_history AS afh,
     fund_history AS fh
WHERE fh.cnpj=afh.fundcnpj
  AND fh.date=afh.date;

-- -----------------------------------------------------
-- View `saldo_diario_and_invest`
-- -----------------------------------------------------
DROP VIEW `saldo_diario_and_invest`;
CREATE VIEW saldo_diario_and_invest AS
SELECT s.accountid AS accountid,
       s.cnpj AS cnpj,
       s.date AS date,
       ahv.value as sum_invested_value,
       s.saldo as saldo
FROM saldo_diario AS s
JOIN
  (SELECT accountid,
          cnpj,
          max(date) AS date,
          count(oper),
          sum(value) AS value
   FROM account_fund_history_value
   GROUP BY accountid,
            cnpj) AS ahv
ON ahv.accountid=s.accountid
AND ahv.cnpj=s.cnpj
AND ahv.date<s.date;

-- -----------------------------------------------------
-- View `saldo_percent_carteira`
-- -----------------------------------------------------
DROP VIEW `saldo_percent_carteira`;
CREATE VIEW saldo_percent_carteira AS
SELECT sd.accountid AS accountid,
       sd.cnpj AS cnpj,
       sd.date AS date,
       saldo_total.saldo AS saldo,
       sd.saldo/saldo_total.saldo AS percent_carteira
FROM saldo_diario AS sd
JOIN
  (SELECT accountid,date,sum(saldo) AS saldo
   FROM saldo_diario
   GROUP BY accountid,date) AS saldo_total ON sd.saldo>0.1
AND saldo_total.accountid=sd.accountid
AND saldo_total.date=sd.date;

-- -----------------------------------------------------
-- View `account_carteira_rendimento_diario`
-- -----------------------------------------------------
DROP VIEW `account_carteira_rendimento_diario`;
CREATE VIEW account_carteira_rendimento_diario AS
SELECT spc.accountid AS accountid,
       spc.date AS date,
       sum(spc.percent_carteira*frd.rendimento_diario) AS rendimento
FROM saldo_percent_carteira AS spc,
     fund_rendimento_diario AS frd
WHERE frd.date=spc.date
  AND spc.cnpj=frd.cnpj
GROUP BY accountid,
         spc.date;

