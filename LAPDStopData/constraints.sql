   ALTER TABLE Officer
ADD CONSTRAINT officer_pk
   PRIMARY KEY (officer_id);

   ALTER TABLE PoliceStop
ADD CONSTRAINT stop_pk
   PRIMARY KEY (stop_id);

   ALTER TABLE Offender
ADD CONSTRAINT offender_pk
   PRIMARY KEY (offender_id);

   ALTER TABLE PoliceStop
ADD CONSTRAINT stop_ofcr1
   FOREIGN KEY (officer1_id)
    REFERENCES Officer(officer_id);

   ALTER TABLE PoliceStop
ADD CONSTRAINT stop_ofcr2
   FOREIGN KEY (officer2_id)
    REFERENCES Officer(officer_id);

   ALTER TABLE Offender
ADD CONSTRAINT offender_stop
   FOREIGN KEY (stop_id)
    REFERENCES PoliceStop(stop_id);
