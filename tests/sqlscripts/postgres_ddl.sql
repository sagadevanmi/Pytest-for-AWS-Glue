CREATE SCHEMA IF NOT EXISTS dbo;
DROP TABLE IF EXISTS dbo.Professor;
DROP TABLE IF EXISTS dbo.Student;
DROP TABLE IF EXISTS dbo.Course;
DROP TABLE IF EXISTS dbo.Dummy;
CREATE TABLE IF NOT EXISTS dbo.Professor(
	professorID int NOT NULL,
	firstname varchar(25) NOT NULL,
	lastname varchar(25) NULL,
	experience int NULL,
    PRIMARY KEY (professorID)
);
CREATE TABLE IF NOT EXISTS dbo.Course(
	courseID int NOT NULL,
	coursename varchar(25) NOT NULL,
	duration int NOT NULL,
    PRIMARY KEY (courseID)
);
CREATE TABLE IF NOT EXISTS dbo.Student(
	studentID int NOT NULL,
	firstname varchar(25) NOT NULL,
	lastname varchar(25) NULL,
	address varchar(500) NOT NULL,
	city varchar(50) NOT NULL,
	state varchar(50) NULL,
	courseID int NOT NULL,
	CONSTRAINT fkcourse
		FOREIGN KEY(courseID)
			REFERENCES dbo.Course(courseID),
    PRIMARY KEY (studentID)
);
CREATE TABLE IF NOT EXISTS dbo.Dummy(
	dummyID int NOT NULL,
	dummyname varchar(25) NOT NULL,
    PRIMARY KEY (dummyID)
);
INSERT INTO dbo.Professor
(professorID, firstname, lastname, experience)
VALUES
(1, 'Priyank', 'Kumar', 5),
(2, 'Dev', 'Dodwani', 6),
(3, 'Nikhil', 'Liyona', 3),
(4, 'Harsh', 'Joshi', 2);
INSERT INTO dbo.Course
(courseID, coursename, duration)
VALUES
(1, 'B.E', 4),
(2, 'B.Sc', 3),
(3, 'M.E', 2);
INSERT INTO dbo.Student
(studentID, firstname, lastname, address, city, state, courseID)
VALUES
(1, 'Sagadevan', 'K', '503, MM Nagar', 'Pune', 'Maharashtra', 1),
(2, 'John', 'F', '201, GB Colony', 'Chennai', 'Tamil Nadu', 1),
(3, 'Priya', 'W', '', 'Lucknow', 'Uttar Pradesh', 1),
(4, 'Jayram', 'M', '', 'Munnar', 'Kerala', 2),
(5, 'Jay', 'B', '', 'Valsad', 'Gujrat', 2),
(6, 'Sam', 'P', '', 'Mumbai', 'Maharashtra', 3),
(7, 'Nick', 'J', '', '', 'Sikkim', 2),
(8, 'Amit', 'K', '', 'Madurai', 'Tamil Nadu', 3),
(9, 'Maddy', 'K', '', 'Hyderabad', 'Telangana', 1),
(10, 'Harish', 'K', '', 'Amritsar', 'Punjab', 3);
INSERT INTO dbo.Dummy
(dummyID, dummyname)
VALUES
(1, 'D1'),
(2, 'Pappu');