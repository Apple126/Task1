SELECT room_name, 
 MAX(date_part('year', age(birthday))) AS age_max, 
 MIN(date_part('year', age(birthday))) AS age_min,
 (MAX(date_part('year', age(birthday))) - MIN(date_part('year', age(birthday)))) AS age_diff
FROM rooms
INNER JOIN students on rooms.id = students.room
GROUP BY room_name
ORDER BY age_diff DESC
LIMIT 5
