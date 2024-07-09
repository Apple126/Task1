SELECT room_name as room, COUNT(students.name) as quantity
FROM rooms
LEFT JOIN students on rooms.id = students.room
GROUP BY room_name
ORDER BY CAST(substring(room_name from '#([0-9]+)') AS INTEGER)
