SELECT DISTINCT(room_name), sex
FROM rooms
LEFT JOIN students on rooms.id = students.room
 
WHERE room_name IN (
 SELECT room_name
 FROM rooms
 LEFT JOIN students on rooms.id = students.room
 WHERE sex = 'M'
)
AND room_name IN (
 SELECT room_name
 FROM rooms
 LEFT JOIN students on rooms.id = students.room
 WHERE sex = 'F'
)
ORDER BY room_name
