-- 1. Database Creation (Optional)
CREATE DATABASE CourseTracker;
 USE CourseTracker;

-- 2. Table Creation

-- Table: students
CREATE TABLE students (
    student_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    registration_date DATE NOT NULL
);

-- 1. Insert Data into students table
INSERT INTO students (student_id, first_name, last_name, email, registration_date) VALUES 
(101, 'Alice', 'Johnson', 'alice.j@mail.com', '2023-01-15'),
(102, 'Bob', 'Smith', 'bob.s@mail.com', '2023-02-20'),
(103, 'Carol', 'Lee', 'carol.l@mail.com', '2023-03-10');

-- Table: courses
CREATE TABLE courses (
    course_id INT PRIMARY KEY AUTO_INCREMENT,
    course_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    total_modules INT NOT NULL 
);


INSERT INTO courses (course_id, course_name, description, total_modules) VALUES 
(501, 'Data Science Fundamentals', 'Intro to Python, Pandas, and ML.', 10),
(502, 'Cloud Computing Basics', 'Concepts of AWS, Azure, and GCP.', 8),
(503, 'Advanced SQL & Databases', 'Deep dive into MySQL and MongoDB.', 12);

-- Table: enrollments (Many-to-Many relationship between students and courses)
CREATE TABLE enrollments (
    enrollment_id INT PRIMARY KEY AUTO_INCREMENT,
    student_id INT NOT NULL,
    course_id INT NOT NULL,
    enrollment_date DATE NOT NULL,
    completion_status ENUM('In Progress', 'Completed', 'Dropped') DEFAULT 'In Progress',
    FOREIGN KEY (student_id) REFERENCES students(student_id),
    FOREIGN KEY (course_id) REFERENCES courses(course_id),
    UNIQUE KEY uk_student_course (student_id, course_id)
);
INSERT INTO enrollments (enrollment_id, student_id, course_id, enrollment_date, completion_status) VALUES 
(1, 101, 501, '2023-09-01', 'In Progress'),
(2, 102, 502, '2023-10-05', 'Completed'),   
(3, 103, 501, '2023-11-10', 'In Progress'), 
(4, 101, 503, '2023-12-01', 'Dropped');

-- Table: progress (Tracks a student's progress within an enrolled course)
CREATE TABLE progress (
    progress_id INT PRIMARY KEY AUTO_INCREMENT,
    enrollment_id INT NOT NULL,
    modules_completed INT NOT NULL DEFAULT 0, -- Modules completed by the student
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (enrollment_id) REFERENCES enrollments(enrollment_id)
);

INSERT INTO progress (progress_id, enrollment_id, modules_completed) VALUES 
(1001, 1, 4),
(1002, 2, 8), 
(1003, 3, 1), 
(1004, 4, 0);

-- 1. Read All Active Enrollments with Progress Details
SELECT
    s.student_id,
    s.first_name,
    s.last_name,
    c.course_name,
    e.enrollment_date,
    e.completion_status,
    p.modules_completed,
    c.total_modules
FROM
    students s
JOIN
    enrollments e ON s.student_id = e.student_id
JOIN
    courses c ON e.course_id = c.course_id
LEFT JOIN
    progress p ON e.enrollment_id = p.enrollment_id
WHERE
    e.completion_status = 'In Progress'; -- Filter for in-progress courses

-- 2. Read All Courses Bob Smith (ID: 102) is Enrolled In
SELECT
    c.course_name,
    e.enrollment_date,
    e.completion_status
FROM
    enrollments e
JOIN
    courses c ON e.course_id = c.course_id
WHERE
    e.student_id = 102;


UPDATE progress
SET
    modules_completed = 4,
    last_updated = CURRENT_TIMESTAMP
WHERE
    enrollment_id = 3;

-- 2. Update Enrollment Status: Alice Johnson (enrollment_id: 4) decides to re-enroll in 'Advanced SQL & Databases'.
UPDATE enrollments
SET
    completion_status = 'In Progress'
WHERE
    enrollment_id = 4;


-- Scenario: Student Bob Smith (ID: 102) is removed from the system.

-- Step 1: Delete progress records for Bob's enrollment(s) (enrollment_id: 2).
DELETE FROM progress
WHERE enrollment_id IN (SELECT enrollment_id FROM enrollments WHERE student_id = 102);

-- Step 2: Delete enrollment records for Bob.
DELETE FROM enrollments
WHERE student_id = 102;

-- Step 3: Delete the student record itself.
DELETE FROM students 
WHERE
    student_id = 102;

DELIMITER //

CREATE PROCEDURE CalculateCompletionPercentage (
    IN p_student_id INT,
    IN p_course_id INT,
    OUT p_completion_percentage DECIMAL(5,2)
)
BEGIN
    DECLARE v_modules_completed INT;
    DECLARE v_total_modules INT;
    DECLARE v_enrollment_id INT;

    -- 1. Find the Enrollment ID for the given student and course
    SELECT
        enrollment_id INTO v_enrollment_id
    FROM
        enrollments
    WHERE
        student_id = p_student_id AND course_id = p_course_id;

    -- 2. Get the progress data and total modules if an enrollment exists
    IF v_enrollment_id IS NOT NULL THEN
        SELECT
            p.modules_completed, c.total_modules
        INTO
            v_modules_completed, v_total_modules
        FROM
            progress p
        JOIN
            enrollments e ON p.enrollment_id = e.enrollment_id
        JOIN
            courses c ON e.course_id = c.course_id
        WHERE
            p.enrollment_id = v_enrollment_id;

        -- 3. Calculate the percentage, handling division by zero
        IF v_total_modules > 0 THEN
            -- Use 100.0 to ensure floating-point division
            SET p_completion_percentage = (v_modules_completed / v_total_modules) * 100.0;
        ELSE
            SET p_completion_percentage = 0.00;
        END IF;

    -- 4. If no enrollment found
    ELSE
        SET p_completion_percentage = NULL;
    END IF;

    -- 5. Optional: Update status to 'Completed' if progress is 100% or more
    IF p_completion_percentage >= 100.00 THEN
        UPDATE enrollments SET completion_status = 'Completed'
        WHERE enrollment_id = v_enrollment_id;
    END IF;

END //

DELIMITER ;


