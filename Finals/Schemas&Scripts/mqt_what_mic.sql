-- MQT
CREATE TABLE mqt_mic_results (manufacturer, model, source_name, style, num_members, size, result_numeric) AS 
	(SELECT manufacturer, model, source_name, style, num_members, size, result_numeric
	FROM factResults
	LEFT JOIN dimMics 
	ON factResults.mic_id = dimMics.mic_id
	LEFT JOIN dimSource
	ON factResults.source_id = dimSource.source_id
	LEFT JOIN dimBand
	ON factResults.band_id = dimBand.band_id
	LEFT JOIN dimVenue 
	ON factResults.venue_id = dimVenue.venue_id)
		DATA INITIALLY DEFERRED
	    REFRESH DEFERRED
	    MAINTAINED BY SYSTEM;
	    
REFRESH TABLE mqt_mic_results;

SELECT * FROM mqt_mic_results LIMIT 500;

-- CUBE 
select model, source_name, style, num_members, size, ROUND(AVG(result_numeric), 4) AS avg_result
from mqt_mic_results 
group by cube(
model, source_name, style, num_members, size)
order by model, source_name, style, num_members, size;