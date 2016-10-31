CREATE TABLE Officer (
    officer_id  NUMBER(5) NOT NULL,
    div_id      VARCHAR(2),
    div_name    VARCHAR2(50),
    PRIMARY KEY (officer_id)
)

CREATE TABLE PoliceStop (
    stop_id            NUMBER(10) NOT NULL,
    stop_date          DATE,
    stop_type          VARCHAR2(50),
    post_stop_activity VARCHAR2(5),
    officer1_id        NUMBER(10),
    officer2_id        NUMBER(10),
    PRIMARY KEY (stop_id),
    FOREIGN KEY (officer1_id) REFERENCES Officer(officer_id),
    FOREIGN KEY (officer2_id) REFERENCES Officer(officer_id)
)

CREATE TABLE Offender (
    offender_id NUMBER(10) NOT NULL,
    gender      VARCHAR2(1),
    ethnicity   VARCHAR2(10),
    stop_id     NUMBER(10),
    PRIMARY KEY (offender_id),
    FOREIGN KEY (stop_id) REFERENCES PoliceStop(stop_id)
)
