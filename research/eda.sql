set search_path to mimiciii;

-- Subjects Admitted more than once
select *
from admissions a
where subject_id in
	(
		select subject_id
		from admissions
		group by subject_id
		having count(*) > 1
	)
order by subject_id

-- look in noteevents... 
select *
from noteevents n 
where n.subject_id = 745
	and n.text like ''

--- Transplant Admission Diagnoses
select *
from diagnoses_icd di
	join d_icd_diagnoses did on di.icd9_code = did.icd9_code
where di.subject_id in (
	select a.subject_id
	from admissions a
	where a.diagnosis like '%TRANSPLANT%'
	order by a.subject_id asc
)

--- Chart Events
SELECT de.icustay_id
    -- , (strftime('%s',de.charttime)-strftime('%s',ie.intime))/60.0/60.0 as HOURS
    , de.charttime
    , ie.intime
    , di.label
    , de.value
    , de.valuenum
    -- , de.uom
    
select de.*, ie.*, di.*
from mimiciii.chartevents de
	INNER join mimiciii.d_items di
		ON de.itemid = di.itemid
	INNER join mimiciii.icustays ie
		ON de.icustay_id = ie.icustay_id
WHERE de.subject_id in (
	select a.subject_id
	from mimiciii.admissions a
	where a.diagnosis like '%TRANSPLANT%'
	order by a.subject_id asc
)
ORDER BY de.charttime;

select *
from mimiciii.labevents
where subject_id = 745