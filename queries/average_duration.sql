select avg(duration) as duration_avg from 
(
select booked.appointment_id as appointment_id, booked.discipline as discipline, TIMESTAMP_DIFF(completed.timestamp_utc, booked.timestamp_utc, MINUTE) as duration from
(select appointment_id, timestamp_utc, discipline from babylon.appointment where type="AppointmentBooked") as booked
join
(select appointment_id, timestamp_utc from babylon.appointment where type="AppointmentComplete")  as completed
on (booked.appointment_id = completed.appointment_id)
)

