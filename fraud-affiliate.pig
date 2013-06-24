set default_parallel 12;



register /home/seller/piggybank.jar;
register /opt/chacha-pig/pig-udfs.jar;

DEFINE AffMatch com.chacha.pig.SelectRegexGroup();
DEFINE LogLoader org.apache.pig.piggybank.storage.apachelog.CombinedLogLoaderWithSession();
DEFINE DayExtractor org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor('yyyy-MM-dd');

logs = LOAD '/hive/warehouse/web_access_logs/access_log.$date.txt*' USING LogLoader as (remoteAddr:chararray, remoteLogname, sessionID, user, time, method,uri:chararray, proto, status, bytes, referer, userAgent);




--FIlter by web page (SR campaign link, gallery, quiz, or question)
--ft = FILTER logs BY uri matches '(.*/askChaCha.*)'; 
--ft = FILTER logs BY uri matches '(.*Omusicawards.com.*)'; 

--Filter by Affiliate ID
ft = FILTER logs BY uri matches '(.*CD13055.*)' OR referer matches '(.*CD13055.*)';



--ft = FILTER logs BY uri matches '(.*which-ex-child-stars-have-gone-wild.*)';
--ft = FILTER logs BY uri matches '(.*5602/what-are-katy-perry-s-hottest-looks.*)';
--ft = FILTER logs BY uri matches '(.*5602/what-are-katy-perry-s-hottest-looks.*)';
--ft = FILTER logs BY uri matches '(.*5602/what-are-katy-perry-s-hottest-looks.*)';
--ft = FILTER logs BY uri matches '(.*5602/what-are-katy-perry-s-hottest-looks.*)';


fe = FOREACH ft GENERATE AffMatch('CD[0-9]{1,20}', 0, uri) as aff, referer, userAgent, remoteAddr, uri;
--fe = FOREACH ft GENERATE AffMatch('CD[0-9]{1,20}', 0, uri) as aff, referer matches '(.*facebook.*)' as facebook, userAgent, remoteAddr, uri;
--fe = FOREACH ft GENERATE referer, uri;


--Group by Referring Domain
gp = GROUP fe BY referer;

gpf = FOREACH gp GENERATE FLATTEN(com.chacha.pig.FirstXBagValues(fe.aff,'1')), FLATTEN(group) , COUNT(fe) as Clicks, FLATTEN(com.chacha.pig.FirstXBagValues(fe.uri,'1'));


STORE gpf INTO 'fraud-affiliate' USING com.chacha.pig.storage.CsvStorage();
 

