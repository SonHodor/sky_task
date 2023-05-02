SELECT 
    sml.id AS lesson_id,
    sm.stream_id,
    c.id AS course_id,
    sm.module_id,
    sml.title AS lesson_title,
    sml.description AS lesson_description,
    sml.start_at AS lesson_start_at,
    sml.end_at AS lesson_end_at,
    sml.homework_url,
    sml.teacher_id,
    sml.online_lesson_join_url,
    sml.online_lesson_recording_url,
    NOW() AS created_at,
    NOW() AS updated_at
FROM 
    stream_module_lesson sml
    JOIN stream_module sm ON sml.stream_module_id = sm.id
    JOIN stream s ON sm.stream_id = s.id
    JOIN course c ON s.course_id = c.id;