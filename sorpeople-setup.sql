# After creating the sorpeople database and tables, use this SQL script to 
# create the Views needed by Grouper

CREATE OR REPLACE VIEW grouper_course_list_all_v AS 
  select distinct concat_ws(':','course',c->>'semester',LOWER(c->>'crsename'),LOWER(c->>'crsenum'),'students_systemOfRecord') AS group_name,
    u.uuid AS "SUBJECT_ID",
    LOWER(c->>'crsename') AS crsename,
    c->>'crsenum' AS crsenum,
    c->>'semester' AS semester
    FROM sorpeople AS t, jsonb_array_elements(t.userdata->'reginfo'->'course') AS c, sorpeople_uuid as u WHERE t.sfuid = u.sfuid;


CREATE OR REPLACE VIEW grouper_course_list_v AS 
  select distinct concat_ws(':','course',c->>'semester',LOWER(c->>'crsename'),LOWER(c->>'crsenum'),LOWER(c->>'sect'),'students_systemOfRecord') AS group_name,
    u.uuid AS "SUBJECT_ID",
    LOWER(c->>'crsename') AS crsename,
    c->>'crsenum' AS crsenum,c->>'sect' as section,
    c->>'semester' AS semester
    FROM sorpeople AS t, jsonb_array_elements(t.userdata->'reginfo'->'course') AS c, sorpeople_uuid as u WHERE t.sfuid = u.sfuid;


CREATE OR REPLACE VIEW grouper_course_list_100level_v AS
  select distinct concat_ws(':','course',c->>'semester',LOWER(c->>'crsename'),'100-level','students_systemOfRecord') AS group_name,
  u.uuid AS "SUBJECT_ID",
  LOWER(c->>'crsename') AS crsename,
  c->>'semester' AS semester
  FROM sorpeople AS t, jsonb_array_elements(t.userdata->'reginfo'->'course') AS c, sorpeople_uuid as u WHERE t.sfuid = u.sfuid and c->>'crsenum' < '200';

CREATE OR REPLACE VIEW grouper_course_list_gradlevel_v AS
  select distinct concat_ws(':','course',c->>'semester',LOWER(c->>'crsename'),'grad-level','students_systemOfRecord') AS group_name,
  u.uuid AS "SUBJECT_ID",
  LOWER(c->>'crsename') AS crsename,
  c->>'semester' AS semester
  FROM sorpeople AS t, jsonb_array_elements(t.userdata->'reginfo'->'course') AS c, sorpeople_uuid as u WHERE t.sfuid = u.sfuid and c->>'crsenum' >= '500';

CREATE OR REPLACE VIEW grouper_course_list_200level_v AS
  select distinct concat_ws(':','course',c->>'semester',LOWER(c->>'crsename'),'200-level','students_systemOfRecord') AS group_name,
  u.uuid AS "SUBJECT_ID",
  LOWER(c->>'crsename') AS crsename,
  c->>'semester' AS semester
  FROM sorpeople AS t, jsonb_array_elements(t.userdata->'reginfo'->'course') AS c, sorpeople_uuid as u WHERE t.sfuid = u.sfuid and c->>'crsenum' >= '200' and c->>'crsenum' < '300';

CREATE OR REPLACE VIEW grouper_course_list_300level_v AS
  select distinct concat_ws(':','course',c->>'semester',LOWER(c->>'crsename'),'300-level','students_systemOfRecord') AS group_name,
  u.uuid AS "SUBJECT_ID",
  LOWER(c->>'crsename') AS crsename,
  c->>'semester' AS semester
  FROM sorpeople AS t, jsonb_array_elements(t.userdata->'reginfo'->'course') AS c, sorpeople_uuid as u WHERE t.sfuid = u.sfuid and c->>'crsenum' >= '300' and c->>'crsenum' < '400';

CREATE OR REPLACE VIEW grouper_course_list_400level_v AS
  select distinct concat_ws(':','course',c->>'semester',LOWER(c->>'crsename'),'400-level','students_systemOfRecord') AS group_name,
  u.uuid AS "SUBJECT_ID",
  LOWER(c->>'crsename') AS crsename,
  c->>'semester' AS semester
  FROM sorpeople AS t, jsonb_array_elements(t.userdata->'reginfo'->'course') AS c, sorpeople_uuid as u WHERE t.sfuid = u.sfuid and c->>'crsenum' >= '400' and c->>'crsenum' < '500';


#Grouper Employees View, replacing illegal chars in group_name:
CREATE OR REPLACE VIEW grouper_employees_v AS
  select distinct concat_ws(':'::text,'basis:affiliations',regexp_replace(lower(j.value ->> 'afflname'::text),'[^a-zA-Z0-9-]','_','g') ) as group_name,
  u.uuid as "SUBJECT_ID",
  j->>'status' as status,
  u.sfuid as sfuid
  FROM sorpeople as t,sorpeople_uuid as u,jsonb_array_elements(t.userdata->'job') as j where j->>'status' <> 'T'  and t.sfuid = u.sfuid and j.value->>'afflname'::text != '';

#Grouper Departments View
CREATE VIEW grouper_depts_v AS 
  select distinct concat_ws(':'::text,'basis:depts:units',j->>'deptcode') as group_name,
  u.uuid as "SUBJECT_ID", 
  j->>'status' as status, 
  u.sfuid as sfuid  
  FROM sorpeople as t,sorpeople_uuid as u,jsonb_array_elements(t.userdata->'job') as j where j->>'status' <> 'T' and j->>'status' <> 'Q' and j->>'status' <> 'R'  and t.sfuid = u.sfuid;
  
#Grouper Academic Plans Views
CREATE OR REPLACE VIEW grouper_academic_plans_v as 
  select distinct concat_ws(':'::text, 'course:plans',regexp_replace(p->>'name'::text,'[^a-zA-Z0-9-]','_','g')) as group_name,
  u.uuid as "SUBJECT_ID" 
  from sorpeople as t, jsonb_array_elements(t.userdata->'reginfo'->'program') as p, sorpeople_uuid as u where t.status='active' and t.source = 'SIMS' and t.sfuid = u.sfuid;  

# Give Grouper user permission to access Views
grant select on grouper_course_list_100level_v to grouperadmin;
grant select on grouper_course_list_200level_v to grouperadmin;
grant select on grouper_course_list_300level_v to grouperadmin;
grant select on grouper_course_list_400level_v to grouperadmin;
grant select on grouper_course_list_gradlevel_v to grouperadmin;
grant select on grouper_course_list_all_v to grouperadmin;
grant select on grouper_course_list_v to grouperadmin;
grant select on grouper_employees_v to grouperadmin;
grant select on grouper_depts_v to grouperadmin;
grant select on grouper_academic_plans_v to grouperadmin;
