SELECT room_name, AVG(date_part('year', age(birthday))) as age
FROM rooms
LEFT JOIN students on rooms.id = students.room
GROUP BY room_name
ORDER BY age
LIMIT 5
