select all_records.appointment_id as appointment_id, all_records.type as status from
(
(select appointment_id, max(timestamp_utc) as ts from babylon.appointment group by appointment_id) as latest_record
join
(select type, appointment_id, timestamp_utc from babylon.appointment) as all_records
on ( (latest_record.appointment_id = all_records.appointment_id) and (latest_record.ts = all_records.timestamp_utc) )
)

