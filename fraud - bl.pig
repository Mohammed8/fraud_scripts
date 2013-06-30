-- This is the Fraud Detection - Black List script, it filters out all bot-traffic from known bot sites

register /home/seller/piggybank.jar;
register /opt/chacha-pig/pig-udfs.jar;

DEFINE AffMatch com.chacha.pig.SelectRegexGroup();
DEFINE LogLoader org.apache.pig.piggybank.storage.apachelog.CombinedLogLoaderWithSession();
DEFINE DayExtractor org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor('yyyy-MM-dd');
DEFINE TimeExtractor org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor('yyyyMMddhhmmss');

logs = LOAD '/hive/warehouse/web_access_logs/access_log.$date.txt*' USING LogLoader as (remoteAddr:chararray, remoteLogname, sessionID, user, time, method,uri:chararray, proto, status, bytes, 
referer, userAgent);


ft = FILTER logs BY referer matches '.*(Sioniam.com|Easyhits4u.com|10khits.com|1stprotraffic|heniut.tk|lotzalove.wen.ru|adf.ly|q.gs|j.gs|track-visits.com|trafficspammer.com|Uphero.com|thebotnet.com|wwe-prize.com|maxvisits.com|hitleap.com|autosurftraffic.com|trafficholder.com|oxosurf.eu|247trafficpro.com|trackstatsnow.com|clixsense.org|cashnhits.com|neobux.com|hits|track|href.li|popads.net|domaingateway.com|youlikehits.com|mydailysitetraffic.net|websyndic.com|autosurf.com.au|hitsuk.co.uk|netvisiteurs.com|autosurfspace.com|sociallylost.com|premium-24.com|hitleap.com|clicksagent.com|planetsportsnews.com|extremeautosurf.com|dreamjobstoday.com|comli.com|ohlays.com|realtrafficsource.com|earneasycash.com|interesting.cc|jerseyshorehq.com|premiumaccounst4u.com|domaingateway.com|hit2donate.com|earn-euro-bux.com|lotofvisitors.com|anoney.com|track-visits|clixsense.com|angege.com).*';

-- Create the Affiliate ID 
fe = FOREACH ft GENERATE  AffMatch('CD[0-9]{1,20}', 0, uri) as aff, TimeExtractor(time) as timestamp, referer, userAgent, remoteAddr, uri;

-- Group by Affiliate
gp = GROUP fe BY aff;

gpf = FOREACH gp GENERATE FLATTEN(group), COUNT(fe) as Clicks, FLATTEN(com.chacha.pig.FirstXBagValues(fe.referer,'1')), FLATTEN(com.chacha.pig.FirstXBagValues(fe.uri,'1'));


STORE gpf INTO 'ootoot' USING com.chacha.pig.storage.CsvStorage();


