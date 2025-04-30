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
    
select ce.*, ie.*, di.*, ad.*, p.*
from mimiciii.chartevents ce
	inner join mimiciii.d_items di
		ON ce.itemid = di.itemid
	inner join mimiciii.icustays ie
		ON ce.icustay_id = ie.icustay_id
	inner join mimiciii.admissions ad
		on ce.hadm_id = ad.hadm_id
	inner join mimiciii.patients p 
		on ce.subject_id = p.subject_id
where ce.subject_id in (
	select a.subject_id
	from mimiciii.admissions a
	where a.diagnosis like E'%TRANSPLANT%'
	order by a.subject_id asc
)
order by ce.charttime;

select * from mimiciii.d_items di 

select ce.*, ie.*, di.*, ad.*, p.*
from mimiciii.chartevents ce
	inner join mimiciii.d_items di
		ON ce.itemid = di.itemid
	inner join mimiciii.icustays ie
		ON ce.icustay_id = ie.icustay_id
	inner join mimiciii.admissions ad
		on ce.hadm_id = ad.hadm_id
	inner join mimiciii.patients p 
		on ce.subject_id = p.subject_id
where ce.subject_id in (
	select a.subject_id
	from mimiciii.admissions a
	where a.diagnosis like '%%TRANSPLANT%%'
	order by a.subject_id asc
)
order by di.label
-- order by ce.charttime

select di.label, count(di.label)
from mimiciii.chartevents ce
	inner join mimiciii.d_items di
		ON ce.itemid = di.itemid
	inner join mimiciii.icustays ie
		ON ce.icustay_id = ie.icustay_id
	inner join mimiciii.admissions ad
		on ce.hadm_id = ad.hadm_id
	inner join mimiciii.patients p 
		on ce.subject_id = p.subject_id
where ce.subject_id in (
	select a.subject_id
	from mimiciii.admissions a
	where a.diagnosis like '%%TRANSPLANT%%'
	order by a.subject_id asc
)
group by di.label

select *
from mimiciii.d_labitems

select le.*, di.*
from mimiciii.labevents le
	inner join mimiciii.d_labitems di
		on di.itemid = di.itemid
where le.subject_id in (
	select a.subject_id
	from mimiciii.admissions a
	where a.diagnosis like '%%TRANSPLANT%%'
	order by a.subject_id asc
)

select * 
from mimiciii.procedures_icd icd
limit 100

select count(*)
from (select distinct(a.subject_id)
		from mimiciii.admissions a
		where a.diagnosis like '%%TRANSPLANT%%'
		order by a.subject_id asc)


select *
from mimiciii.labevents
where subject_id = 745