CREATE TABLE Officer (
    officer_id  NUMBER(5) NOT NULL,
    div_id      VARCHAR(2),
    div_name    VARCHAR2(50)
)

CREATE TABLE PoliceStop (
    stop_id            NUMBER(10) NOT NULL,
    stop_date          TIMESTAMP(6),
    stop_type          VARCHAR2(3),
    post_stop_activity VARCHAR2(1),
    officer1_id        NUMBER(10),
    officer2_id        NUMBER(10)
)

CREATE TABLE Offender (
    offender_id NUMBER(10) NOT NULL,
    gender      VARCHAR2(1),
    ethnicity   VARCHAR2(20),
    stop_id     NUMBER(10)
)
