import java.io.*;
import java.util.*;
import java.sql.*;

public class JdbcOracleConnectionTemplate {

    // 1) DROP views then tables
    private static final String DROP_SQL =
        "DROP VIEW v_comp_stats CASCADE CONSTRAINTS;\n"
      + "DROP VIEW v_app_detail CASCADE CONSTRAINTS;\n"
      + "DROP VIEW v_posting_summary CASCADE CONSTRAINTS;\n"
      + "DROP VIEW view_ON_apps CASCADE CONSTRAINTS;\n"
      + "DROP TABLE company_review CASCADE CONSTRAINTS;\n"
      + "DROP TABLE student_application CASCADE CONSTRAINTS;\n"
      + "DROP TABLE professional_application CASCADE CONSTRAINTS;\n"
      + "DROP TABLE application CASCADE CONSTRAINTS;\n"
      + "DROP TABLE transcript_line CASCADE CONSTRAINTS;\n"
      +"DROP TABLE transcript CASCADE CONSTRAINTS;\n"
      + "DROP TABLE work_experience CASCADE CONSTRAINTS;\n"
      + "DROP TABLE internship_experience CASCADE CONSTRAINTS;\n"
      + "DROP TABLE experience CASCADE CONSTRAINTS;\n"
      + "DROP TABLE educational_degree CASCADE CONSTRAINTS;\n"
      + "DROP TABLE fulltime_posting CASCADE CONSTRAINTS;\n"
      + "DROP TABLE parttime_posting CASCADE CONSTRAINTS;\n"
      + "DROP TABLE internship_posting CASCADE CONSTRAINTS;\n"
      + "DROP TABLE job_posting CASCADE CONSTRAINTS;\n"
      + "DROP TABLE hiring_manager CASCADE CONSTRAINTS;\n"
      + "DROP TABLE applicant CASCADE CONSTRAINTS;\n"
      + "DROP TABLE company CASCADE CONSTRAINTS;\n";

   // 2) CREATE TABLES
private static final String CREATE_SQL =
  "CREATE TABLE applicant (\n"
+ "  user_id         VARCHAR2(30) PRIMARY KEY,\n"
+ "  name            VARCHAR2(120)   NOT NULL,\n"
+ "  email           VARCHAR2(300)   NOT NULL,\n"
+ "  phone           VARCHAR2(20),\n"
+ "  street_address  VARCHAR2(200)   NOT NULL,\n"
+ "  city            VARCHAR2(200)   NOT NULL,\n"
+ "  province        VARCHAR2(200)   NOT NULL,\n"
+ "  postal_code     VARCHAR2(10)    NOT NULL,\n"
+ "  country         VARCHAR2(25)    NOT NULL,\n"
+ "  CONSTRAINT email_chek CHECK (REGEXP_LIKE(email, '.+@.+'))\n"
+ ");\n"

+ "CREATE TABLE company (\n"
+ "  company_id  NUMBER PRIMARY KEY,\n"
+ "  name        VARCHAR2(120) NOT NULL,\n"
+ "  office      VARCHAR2(120) NOT NULL\n"
+ ");\n"

+ "CREATE TABLE hiring_manager (\n"
+ "  admin_id    NUMBER PRIMARY KEY,\n"
+ "  name        VARCHAR2(120) NOT NULL,\n"
+ "  email       VARCHAR2(120) NOT NULL,\n"
+ "  company_id  NUMBER NOT NULL,\n"
+ "  FOREIGN KEY (company_id) REFERENCES company(company_id)\n"
+ ");\n"

+ "CREATE TABLE educational_degree (\n"
+ "  degree_id   NUMBER PRIMARY KEY,\n"
+ "  user_id     VARCHAR2(30) NOT NULL,\n"
+ "  type        VARCHAR2(80)  NOT NULL,\n"
+ "  major       VARCHAR2(120) NOT NULL,\n"
+ "  institution VARCHAR2(120) NOT NULL,\n"
+ "  FOREIGN KEY (user_id) REFERENCES applicant(user_id)\n"
+ ");\n"

+ "CREATE TABLE transcript (\n"
+ "  transcript_id NUMBER PRIMARY KEY,\n"
+ "  degree_id     NUMBER NOT NULL,\n"
+ "  t_name        VARCHAR2(200) NOT NULL,\n"
+ "  gpa           NUMBER(3,2)   NOT NULL,\n"
+ "  CONSTRAINT gpa_range_chk CHECK (gpa BETWEEN 0.00 AND 4.00),\n"
+ "  FOREIGN KEY (degree_id) REFERENCES educational_degree(degree_id)\n"
+ ");\n"

+ "CREATE TABLE transcript_line (\n"
+ "  transcript_id NUMBER NOT NULL,\n"
+ "  course        VARCHAR2(200) NOT NULL,\n"
+ "  grade         VARCHAR2(4)   NOT NULL,\n"
+ "  class_avg     NUMBER(5,2),\n"
+ "  CONSTRAINT class_avg_chk CHECK (class_avg IS NULL OR (class_avg BETWEEN 0 AND 100)),\n"
+ "  CONSTRAINT grade_in_chk CHECK (grade IS NULL OR grade IN ('A+','A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F','PASS','FAIL')),\n"
+ "  PRIMARY KEY (transcript_id, course),\n"
+ "  FOREIGN KEY (transcript_id) REFERENCES transcript(transcript_id)\n"
+ ");\n"

+ "CREATE TABLE experience (\n"
+ "  experience_id       NUMBER PRIMARY KEY,\n"
+ "  user_id             VARCHAR2(30) NOT NULL,\n"
+ "  company_name        VARCHAR2(200) NOT NULL,\n"
+ "  role                VARCHAR2(120) NOT NULL,\n"
+ "  start_date          DATE          NOT NULL,\n"
+ "  end_date            DATE,\n"
+ "  CONSTRAINT end_date_chek CHECK (end_date IS NULL OR end_date >= start_date),\n"
+ "  FOREIGN KEY (user_id) REFERENCES applicant(user_id)\n"
+ ");\n"

+ "CREATE TABLE work_experience(\n"
+ "    experience_id NUMBER PRIMARY KEY\n"
+ "        REFERENCES experience(experience_id)\n"
+ ");\n"

+ "CREATE TABLE internship_experience(\n"
+ "    experience_id NUMBER PRIMARY KEY\n"
+ "        REFERENCES experience(experience_id),\n"
+ "    mentor_name VARCHAR2(200) NOT NULL\n"
+ ");\n"

+ "CREATE TABLE job_posting (\n"
+ "  job_id       VARCHAR2(10) PRIMARY KEY,\n"
+ "  admin_id     NUMBER NOT NULL,\n"
+ "  title        VARCHAR2(200)  NOT NULL,\n"
+ "  description  CLOB NOT NULL,\n"
+ "  requirements CLOB NOT NULL,\n"
+ "  salary_min   NUMBER (12,2),\n"
+ "  salary_max   NUMBER (12,2),\n"
+ "  posted_date  DATE           NOT NULL,\n"
+ "  CONSTRAINT salary_min_chek CHECK (salary_min IS NULL OR salary_min >= 0),\n"
+ "  CONSTRAINT salary_max_chek CHECK (salary_max IS NULL OR salary_max >= 0),\n"
+ "  CONSTRAINT salary_range_chek CHECK( salary_min IS NULL OR salary_max IS NULL OR salary_max >= salary_min),\n"
+ "  FOREIGN KEY (admin_id)  REFERENCES hiring_manager(admin_id)\n"
+ ");\n"

+ "CREATE TABLE fulltime_posting(\n"
+ "    job_id VARCHAR2(10) PRIMARY KEY\n"
+ "        REFERENCES job_posting(job_id),\n"
+ "    hours_per_week NUMBER(4,1) NOT NULL,\n"
+ "    benefits_plan VARCHAR2(200) NOT NULL,\n"
+ "    CONSTRAINT hours_full_chek CHECK (hours_per_week BETWEEN 31 AND 40)\n"
+ ");\n"

+ "CREATE TABLE parttime_posting(\n"
+ "    job_id VARCHAR2(10) PRIMARY KEY\n"
+ "        REFERENCES job_posting(job_id),\n"
+ "    hours_per_week NUMBER(4,1) NOT NULL,\n"
+ "    CONSTRAINT hours_part_chek CHECK (hours_per_week BETWEEN 1 AND 30)\n"
+ ");\n"

+ "CREATE TABLE internship_posting(\n"
+ "    job_id VARCHAR2(10) PRIMARY KEY\n"
+ "        REFERENCES job_posting(job_id),\n"
+ "    term_length     NUMBER(2) NOT NULL,\n"
+ "    school_credit   VARCHAR2(1) NOT NULL,\n"
+ "    CONSTRAINT term_length_chek CHECK (term_length BETWEEN 1 AND 24),\n"
+ "    CONSTRAINT credit_chek CHECK (school_credit IN ('Y', 'N'))\n"
+ ");\n"

+ "CREATE TABLE application (\n"
+ "  application_id   NUMBER PRIMARY KEY,\n"
+ "  user_id          VARCHAR2(30) NOT NULL,\n"
+ "  job_id           VARCHAR2(10) NOT NULL,\n"
+ "  status           VARCHAR2(20)  NOT NULL,\n"
+ "  application_date DATE          NOT NULL,\n"
+ "  CONSTRAINT status_chek CHECK (status IN ('SUBMITTED','UNDER REVIEW','INTERVIEWING','OFFER','REJECTED','WITHDRAWN')),\n"
+ "  FOREIGN KEY (user_id) REFERENCES applicant(user_id),\n"
+ "  FOREIGN KEY (job_id)  REFERENCES job_posting(job_id)\n"
+ ");\n"

+ "CREATE TABLE student_application(\n"
+ "    application_id NUMBER PRIMARY KEY\n"
+ "        REFERENCES application(application_id),\n"
+ "    term_availability   VARCHAR2(40) NOT NULL,\n"
+ "    graduation_year     NUMBER NOT NULL,\n"
+ "    advisor_email       VARCHAR2(120) NOT NULL\n"
+ ");\n"

+ "CREATE TABLE professional_application(\n"
+ "  application_id NUMBER PRIMARY KEY\n"
+ "      REFERENCES application(application_id),\n"
+ "  years_experience    NUMBER NOT NULL,\n"
+ "  current_title       VARCHAR2(120) NOT NULL,\n"
+ "  current_company     VARCHAR2(120) NOT NULL\n"
+ ");\n"

+ "CREATE TABLE company_review (\n"
+ "  review_id   NUMBER PRIMARY KEY,\n"
+ "  company_id  NUMBER NOT NULL,\n"
+ "  user_id     VARCHAR2(30) NOT NULL,\n"
+ "  satisfaction_rate NUMBER(2,1),\n"
+ "  hiring_rate NUMBER(2,1),\n"
+ "  posted_on   DATE NOT NULL,\n"
+ "  CONSTRAINT satis_chek CHECK (satisfaction_rate IS NULL OR satisfaction_rate BETWEEN 1.0 AND 5.0),\n"
+ "  CONSTRAINT hire_chek CHECK (hiring_rate IS NULL OR hiring_rate BETWEEN 1.0 AND 5.0),\n"
+ "  FOREIGN KEY (company_id) REFERENCES company(company_id),\n"
+ "  FOREIGN KEY (user_id)    REFERENCES applicant(user_id)\n"
+ ");\n";



    // 3) POPULATE TABLES
private static final String POPULATE_SQL =
"INSERT INTO applicant VALUES ('msidd', 'Musheer Siddiqui', 'musheer@gmail.com', '+14164164164', '123 Dundas St', 'Toronto', 'ON', 'A3E 1U5', 'Canada');\n"
+ "INSERT INTO applicant VALUES ('alex39', 'Alex Smith', 'a39smith@hotmail.com', '+16478129384', 'Av Gustave Eiffel', 'Paris', 'N/A', '75007', 'France');\n"
+ "INSERT INTO applicant VALUES ('jchan22', 'Samantha Chan', 'j.chan22@gmail.com', '+14167778888', '789 Bay St', 'Toronto', 'ON', 'M5G 2C9', 'Canada');\n"
+ "INSERT INTO applicant VALUES ('rahulk', 'Rahul Kumar', 'rkumar@hotmail.com', '+919876543210', '11 Mumbai Road', 'Bangalore', 'KA', '560001', 'India');\n"
+ "INSERT INTO company VALUES (1, 'Google Toronto', 'Toronto');\n"
+ "INSERT INTO company VALUES (42, 'Shopify USA', 'Los Angeles');\n"
+ "INSERT INTO company VALUES (77, 'IBM Canada', 'Ottawa');\n"
+ "INSERT INTO company VALUES (88, 'Microsoft France', 'Paris');\n"
+ "INSERT INTO hiring_manager VALUES (110, 'Joe Lawrence', 'joe.law@google.com', 1);\n"
+ "INSERT INTO hiring_manager VALUES (23, 'Remy Patel', 'r18patel@shopify.careers.com', 42);\n"
+ "INSERT INTO hiring_manager VALUES (300, 'John Wong', 'john.wong@ibm.com', 77);\n"
+ "INSERT INTO hiring_manager VALUES (400, 'Louis Martin', 'louis.martin@microsoft.com', 88);\n"
+ "INSERT INTO educational_degree VALUES (3022, 'msidd', 'BEng', 'Software Engineering','Toronto Metropolitan University');\n"
+ "INSERT INTO educational_degree VALUES (1034, 'alex39', 'BSc', 'Computer Science', 'University of Toronto');\n"
+ "INSERT INTO educational_degree VALUES (4090, 'jchan22', 'BCom', 'Information Systems', 'York University');\n"
+ "INSERT INTO educational_degree VALUES (5099, 'rahulk', 'BTech', 'Electronics', 'IIT Delhi');\n"

  // parent transcript
+ "INSERT INTO transcript VALUES (2000, 3022, 'Fall 2023', 3.5);\n"
+ "INSERT INTO transcript VALUES (2001, 3022, 'Fall 2023', 4.0);\n"
+ "INSERT INTO transcript VALUES (2034, 1034, 'Spring 2020', 3.8);\n"
+ "INSERT INTO transcript VALUES (2035, 4090, 'Winter 2024', 3.0);\n"
+ "INSERT INTO transcript VALUES (2036, 5099, 'Fall 2019', 3.7);\n"

  // course entries
+ "INSERT INTO transcript_line VALUES (2000, 'Database Systems I','B+',78.0);\n"
+ "INSERT INTO transcript_line VALUES (2001, 'Electronic Circuits','A+',100.0);\n"
+ "INSERT INTO transcript_line VALUES (2034, 'Database Systems I','A',90.1);\n"
+ "INSERT INTO transcript_line VALUES (2035, 'Business Analytics','B',82.0);\n"
+ "INSERT INTO transcript_line VALUES (2036, 'Digital Logic','A-',88.0);\n"

+ "INSERT INTO experience VALUES (3001, 'msidd', 'Walmart','Business Analyst', DATE '2020-04-02', DATE '2021-10-20');\n"
+ "INSERT INTO experience VALUES (3002, 'alex39', 'Costco','Pharmacy Technician', DATE '2023-07-21', DATE '2023-09-10');\n"
+ "INSERT INTO experience VALUES (3003, 'jchan22', 'TD Bank','IT Support', DATE '2021-01-01', DATE '2022-06-30');\n"
+ "INSERT INTO experience VALUES (3004, 'rahulk', 'Infosys','Software Engineer', DATE '2019-02-01', DATE '2021-12-31');\n"

+ "INSERT INTO work_experience VALUES (3002);\n"
+ "INSERT INTO work_experience VALUES (3004);\n"

+ "INSERT INTO internship_experience VALUES (3001, 'Jonah Walters');\n"
+ "INSERT INTO internship_experience VALUES (3003, 'Karen White');\n"

+ "INSERT INTO job_posting VALUES ('GOOCYB0023', 110, 'Cybersecurity Analyst', 'Assist Cybersecurity team on patching up points of data leaks', 'Data Analytics Knowledge', 60000.00, 85000.00, DATE '2026-01-01');\n"
+ "INSERT INTO job_posting VALUES ('SHOHRA0023', 23, 'HR Assistant', 'Work with HR lead to faciliate internal meetings', 'Knowledge in technology', 55000.00, 60000.00, DATE '2026-04-05');\n"
+ "INSERT INTO job_posting VALUES ('IBMDEV0044', 300, 'Java Developer', 'Develop enterprise systems', 'Java, SQL skills', 70000.00, 90000.00, DATE '2026-02-15');\n"
+ "INSERT INTO job_posting VALUES ('MICANA1122', 400, 'Data Analyst', 'Analyze sensitive client data', 'Python, Excel, Security Clearance', 65000.00, 80000.00, DATE '2026-03-10');\n"

+ "INSERT INTO fulltime_posting VALUES ('GOOCYB0023', 37.5, 'Health + Dental');\n"
+ "INSERT INTO fulltime_posting VALUES ('IBMDEV0044', 38.0, 'Pension + Dental');\n"
+ "INSERT INTO parttime_posting VALUES ('SHOHRA0023', 20);\n"
+ "INSERT INTO parttime_posting VALUES ('MICANA1122', 25);\n"
+ "INSERT INTO internship_posting VALUES ('SHOHRA0023', 4, 'Y');\n"
+ "INSERT INTO internship_posting VALUES ('IBMDEV0044', 6, 'N');\n"

+ "INSERT INTO application VALUES (4001, 'msidd',   'GOOCYB0023', 'SUBMITTED',    DATE '2025-09-20');\n"
+ "INSERT INTO application VALUES (4002, 'alex39',  'SHOHRA0023', 'UNDER REVIEW', DATE '2025-08-10');\n"
+ "INSERT INTO application VALUES (4003, 'jchan22', 'IBMDEV0044', 'INTERVIEWING', DATE '2025-09-25');\n"
+ "INSERT INTO application VALUES (4004, 'rahulk',  'MICANA1122', 'OFFER',        DATE '2025-10-01');\n"

+ "INSERT INTO student_application VALUES (4001, 'Fall 2024', 2025, 'alessa@torontomu.ca');\n"
+ "INSERT INTO student_application VALUES (4003, 'Winter 2025', 2026, 'meliza.max@yorku.ca');\n"
+ "INSERT INTO professional_application VALUES (4002, 9, 'QA Analyst', 'Shopify');\n"
+ "INSERT INTO professional_application VALUES (4004, 4, 'Data Analyst', 'Infosys');\n"
+ "INSERT INTO company_review VALUES (5001, 1,  'msidd',   4.5, 4.0, DATE '2025-09-01');\n"
+ "INSERT INTO company_review VALUES (5002, 42, 'alex39',  3.8, 3.5, DATE '2024-10-06');\n"
+ "INSERT INTO company_review VALUES (5003, 77, 'jchan22', 4.2, 4.5, DATE '2025-05-15');\n"
+ "INSERT INTO company_review VALUES (5004, 88, 'rahulk',  4.0, 3.9, DATE '2025-07-22');\n"
+ "COMMIT;\n";
      


    // QUERY 1: Company Leaderboard
private static final String Q1_SQL =
      "SELECT\n"
    + "    c.company_id,\n"
    + "    c.name AS company_name,\n"
    + "    r.avg_satisfaction,\n"
    + "    r.avg_hiring_rate,\n"
    + "    r.review_count,\n"
    + "    p.open_postings,\n"
    + "    p.avg_salary_mid\n"
    + "FROM company c\n"
    + "JOIN (\n"
    + "    SELECT company_id,\n"
    + "           AVG(satisfaction_rate) AS avg_satisfaction,\n"
    + "           AVG(hiring_rate)       AS avg_hiring_rate,\n"
    + "           COUNT(*)               AS review_count\n"
    + "    FROM company_review\n"
    + "    GROUP BY company_id\n"
    + ") r ON r.company_id = c.company_id\n"
    + "JOIN (\n"
    + "    SELECT hm.company_id,\n"
    + "           COUNT(DISTINCT jp.job_id)         AS open_postings,\n"
    + "           AVG((jp.salary_min+jp.salary_max)/2) AS avg_salary_mid\n"
    + "    FROM job_posting jp\n"
    + "    JOIN hiring_manager hm ON hm.admin_id=jp.admin_id\n"
    + "    GROUP BY hm.company_id\n"
    + ") p ON p.company_id = c.company_id\n"
    + "ORDER BY r.avg_satisfaction DESC, p.open_postings DESC;\n";

    // QUERY 2: Applicant Academic Profile
private static final String Q2_SQL =
      "WITH gpa_per_user AS (\n"
    + "    SELECT ed.user_id, AVG(t.gpa) AS avg_gpa\n"
    + "    FROM educational_degree ed\n"
    + "    JOIN transcript t ON t.degree_id = ed.degree_id\n"
    + "    GROUP BY ed.user_id\n"
    + "),\n"
    + "exp_count AS (\n"
    + "    SELECT e.user_id, COUNT(*) AS exp_count\n"
    + "    FROM experience e\n"
    + "    GROUP BY e.user_id\n"
    + ")\n"
    + "SELECT\n"
    + "  a.user_id,\n"
    + "  a.name,\n"
    + "  g.avg_gpa,\n"
    + "  e.exp_count,\n"
    + "  CASE WHEN EXISTS(\n"
    + "    SELECT 1 FROM experience e2\n"
    + "    JOIN work_experience we ON we.experience_id = e2.experience_id\n"
    + "    WHERE e2.user_id = a.user_id\n"
    + "  ) THEN 'Y' ELSE 'N' END AS has_work_exp,\n"
    + "  CASE WHEN EXISTS(\n"
    + "    SELECT 1 FROM experience e3\n"
    + "    JOIN internship_experience ie ON ie.experience_id = e3.experience_id\n"
    + "    WHERE e3.user_id = a.user_id\n"
    + "  ) THEN 'Y' ELSE 'N' END AS has_intern_exp\n"
    + "FROM applicant a\n"
    + "JOIN gpa_per_user g ON g.user_id = a.user_id\n"
    + "JOIN exp_count e    ON e.user_id = a.user_id\n"
    + "ORDER BY g.avg_gpa, e.exp_count DESC;\n";

    // QUERY 3: Hiring Manager Workload
private static final String Q3_SQL =
  "WITH pc AS (\n"
+ "  SELECT jp.admin_id,\n"
+ "         COUNT(DISTINCT jp.job_id)            AS postings,\n"
+ "         AVG((jp.salary_min+jp.salary_max)/2) AS avg_salary_mid,\n"
+ "         MAX(jp.posted_date)                  AS last_posted\n"
+ "  FROM job_posting jp\n"
+ "  GROUP BY jp.admin_id\n"
+ "),\n"
+ "ac AS (\n"
+ "  SELECT hm.admin_id,\n"
+ "         COUNT(*) AS app_count,\n"
+ "         SUM(CASE WHEN ap.status = 'UNDER REVIEW' THEN 1 ELSE 0 END) AS under_review,\n"
+ "         SUM(CASE WHEN ap.status = 'INTERVIEWING' THEN 1 ELSE 0 END) AS interviewing,\n"
+ "         SUM(CASE WHEN ap.status = 'OFFER'        THEN 1 ELSE 0 END) AS offers\n"
+ "  FROM application ap\n"
+ "  JOIN job_posting jp ON jp.job_id = ap.job_id\n"
+ "  JOIN hiring_manager hm ON hm.admin_id = jp.admin_id\n"
+ "  GROUP BY hm.admin_id\n"
+ ")\n"
+ "SELECT\n"
+ "  hm.admin_id,\n"
+ "  c.company_id,\n"
+ "  c.name AS company_name,\n"
+ "  pc.postings,\n"
+ "  ac.app_count,\n"
+ "  (ac.app_count / pc.postings) AS apps_per_posting,\n"
+ "  ac.under_review,\n"
+ "  ac.interviewing,\n"
+ "  ac.offers,\n"
+ "  pc.avg_salary_mid,\n"
+ "  pc.last_posted\n"
+ "FROM hiring_manager hm\n"
+ "JOIN company c ON c.company_id = hm.company_id\n"
+ "JOIN pc ON pc.admin_id = hm.admin_id\n"
+ "JOIN ac ON ac.admin_id = hm.admin_id\n"
+ "ORDER BY ac.app_count DESC, apps_per_posting DESC;\n";

    // QUERY 4: Application-Company Allocation
private static final String Q4_SQL =
      "SELECT\n"
    + "  a.user_id,\n"
    + "  a.name AS applicant_name,\n"
    + "  c.company_id,\n"
    + "  c.name AS company_name,\n"
    + "  ap.status\n"
    + "FROM application ap\n"
    + "JOIN applicant a   ON a.user_id    = ap.user_id\n"
    + "JOIN job_posting jp ON jp.job_id    = ap.job_id\n"
    + "JOIN hiring_manager hm ON hm.admin_id = jp.admin_id\n"
    + "JOIN company c      ON c.company_id = hm.company_id\n"
    + "JOIN (\n"
    + "  SELECT company_id, AVG(satisfaction_rate) AS avg_satis\n"
    + "  FROM company_review\n"
    + "  GROUP BY company_id\n"
    + ") cr ON cr.company_id = c.company_id\n"
    + "WHERE ap.status IN ('UNDER REVIEW','INTERVIEWING','OFFER')\n"
    + "  AND cr.avg_satis > (SELECT AVG(satisfaction_rate) FROM company_review)\n"
    + "ORDER BY cr.avg_satis DESC, c.company_id, a.user_id;\n";

    // QUERY 5: Postings with No Applications
private static final String Q5_SQL =
      "SELECT\n"
    + "  jp.job_id,\n"
    + "  c.name AS company_name,\n"
    + "  jp.title,\n"
    + "  CASE\n"
    + "    WHEN EXISTS (SELECT 1 FROM fulltime_posting   fp WHERE fp.job_id = jp.job_id) THEN 'FULLTIME'\n"
    + "    WHEN EXISTS (SELECT 1 FROM parttime_posting   pp WHERE pp.job_id = jp.job_id) THEN 'PARTTIME'\n"
    + "    WHEN EXISTS (SELECT 1 FROM internship_posting ip WHERE ip.job_id = jp.job_id) THEN 'INTERNSHIP'\n"
    + "    ELSE 'UNKNOWN'\n"
    + "  END AS posting_type\n"
    + "FROM job_posting jp\n"
    + "JOIN hiring_manager hm ON hm.admin_id = jp.admin_id\n"
    + "JOIN company c ON c.company_id = hm.company_id\n"
    + "WHERE NOT EXISTS (\n"
    + "  SELECT 1 FROM application ap WHERE ap.job_id = jp.job_id\n"
    + ")\n"
    + "ORDER BY company_name, jp.job_id;\n";


    public static void main(String[] args) {

        Connection conn1 = null; 

        Scanner in = new Scanner(System.in);
        Console console = System.console();

        System.out.print("Oracle user: ");
        String user = in.nextLine().trim();

        String pass = null;
        if (console != null) {
            char[] pw = console.readPassword("Oracle password: ");
            if (pw != null){
                pass = new String(pw);
            }
        }

        if (pass == null){
            System.out.print("Oracle password: ");
            pass = in.nextLine();
        }

        System.out.print("DSN (USE: oracle.scs.ryerson.ca:1521:orcl): ");
        String dsn = in.nextLine().trim();

        try {
            //Loading Oracle JDBC from ojdbc17.jar
            Class.forName("oracle.jdbc.OracleDriver");

            String dbURL1 = "jdbc:oracle:thin:@" + dsn;

            Properties props = new Properties();
            props.put("user", user);
            props.put("password", pass);

            conn1 = DriverManager.getConnection(dbURL1, props);
            conn1.setAutoCommit(false);

            System.out.println("\nConnected as: " + user + "\n");

            //Menu
            while (true) {
                System.out.println("=================================================================");
                System.out.println("| Job Board Database Management System                               |");
                System.out.println("| Main Menu - Select Desired Operation(s):                       |");
                System.out.println("-----------------------------------------------------------------");
                System.out.println(" 1) Drop Tables/Views");
                System.out.println(" 2) Create Tables");
                System.out.println(" 3) Populate Tables");
                System.out.println(" 4) QUERY 1: Company Leaderboard");
                System.out.println(" 5) QUERY 2: Applicant Academic Profile");
                System.out.println(" 6) QUERY 3: Hiring Manager Workload");
                System.out.println(" 7) QUERY 4: Application-Company Allocation");
                System.out.println(" 8) QUERY 5: Postings with No Applications");
                System.out.println(" 9) Manage Applicants");
                System.out.println(" 10) Manage Postings");
                System.out.println(" E) End/Exit");
                System.out.print("Choose: ");
                String choice = in.nextLine().trim();
                    switch (choice.toUpperCase()) {
                        case "1":
                            runSqlBatch(conn1, DROP_SQL, true, false);
                            System.out.println("Done: drops attempted.\n");
                            break;
                        case "2":
                            runSqlBatch(conn1, CREATE_SQL, false, false);
                            System.out.println("Done: tables created.\n");
                            break;
                        case "3":
                            runSqlBatch(conn1, POPULATE_SQL, false, false);
                            conn1.commit();
                            System.out.println("Done: tables populated (committed).\n");
                            break;
                        case "4":
                            //Q1
                            runSqlBatch(conn1, Q1_SQL, false, true);
                            break;
                        case "5":
                            //Q2
                            runSqlBatch(conn1, Q2_SQL, false, true);
                            break;
                        case "6":
                            //Q3
                            runSqlBatch(conn1, Q3_SQL, false, true);
                            break;    
                        case "7":
                            //Q4
                            runSqlBatch(conn1, Q4_SQL, false, true);
                            break;
                        case "8":
                            //Q5
                            runSqlBatch(conn1, Q5_SQL, false, true);
                            break;
                        case "9":
                            manageApplicants(conn1, in);
                            break;
                        case "10":
                            manageJobPostings(conn1, in);
                            break;
                        case "E":
                            System.out.println("Logged out, Bye!");
                            return;
                        default:
                            System.out.println("Invalid Input.\n");
                    }
                } 

        } catch (ClassNotFoundException e) {
            System.err.println("Oracle JDBC driver not found. JDBC needs to be in the project folder.");
            e.printStackTrace();
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (conn1 != null && !conn1.isClosed()) {
                    conn1.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

//Application Manager MENU AND OPTION METHODS
private static void manageApplicants(Connection conn, Scanner in) throws SQLException {
    while (true){
        System.out.println("\n Welcome to the Applicant Manager!");
        System.out.println("1 Add Applicant");
        System.out.println("2. Delete Applicant");
        System.out.println("3. Update Applicatn Email");
        System.out.println("4. Search Applicant (by user ID number)");
        System.out.println("B. Main Menu");
        System.out.println("Choose: ");
        String choice = in.nextLine().trim().toUpperCase();
            switch(choice){
                case "1":
                    addApplicant(conn, in);
                    break;
                case "2":
                deleteApplicant(conn,in);
                break;
                case "3":
                updateApplicantEmail(conn,in);
                break;
                case "4":
                searchApplicant(conn,in);
                break;
                case "B":
                    return;
                    default:
                    System.out.println("Invalid Input\n");
            }       
}
}

private static void addApplicant(Connection conn, Scanner in) throws SQLException {
    System.out.println("\n--- Add Applicant (with Degree, Transcript, and Experience) ---");
    System.out.print("user_id: ");
    String userId = in.nextLine().trim();

    System.out.print("Name: ");
    String name = in.nextLine().trim();
    System.out.print("Email: ");
    String email = in.nextLine().trim();

    System.out.print("Phone (blank = NULL): ");
    String phone = in.nextLine().trim();
    if (phone.isEmpty()) phone = null;

    System.out.print("Street Address: ");
    String street = in.nextLine().trim();

    System.out.print("City: ");
    String city = in.nextLine().trim();

    System.out.print("Province/State: ");
    String province = in.nextLine().trim();

    System.out.print("Postal Code: ");
    String postal = in.nextLine().trim();

    System.out.print("Country: ");
    String country = in.nextLine().trim();

    String sqlApplicant = "INSERT INTO applicant "
            + "(user_id, name, email, phone, street_address, city, province, postal_code, country) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement ps = conn.prepareStatement(sqlApplicant)) {
        ps.setString(1, userId);
        ps.setString(2, name);
        ps.setString(3, email);

        if (phone == null) {
            ps.setNull(4, java.sql.Types.VARCHAR);
        } else {
            ps.setString(4, phone);
        }

        ps.setString(5, street);
        ps.setString(6, city);
        ps.setString(7, province);
        ps.setString(8, postal);
        ps.setString(9, country);

        int rows = ps.executeUpdate();
        if (rows == 0) {
            System.out.println("No applicant inserted.\n");
            conn.rollback();
            return;
        }
    }

    addDegreeAndTranscriptForUser(conn, in, userId);
    addExperienceForUser(conn, in, userId);

    conn.commit();
    System.out.println("applicant and info added.\n");
}

private static void addDegreeAndTranscriptForUser(Connection conn, Scanner in, String userId) throws SQLException {
    System.out.println("\nAdd Educational Degree for " + userId);

    System.out.print("degree_id (NUMBER): ");
    int degreeId = Integer.parseInt(in.nextLine().trim());

    System.out.print("Degree type (e.g., BSc, BEng): ");
    String type = in.nextLine().trim();

    System.out.print("Major: ");
    String major = in.nextLine().trim();

    System.out.print("Institution: ");
    String institution = in.nextLine().trim();

    String sqlDeg = "INSERT INTO educational_degree "
            + "(degree_id, user_id, type, major, institution) "
            + "VALUES (?, ?, ?, ?, ?)";

    try (PreparedStatement ps = conn.prepareStatement(sqlDeg)) {
        ps.setInt(1, degreeId);
        ps.setString(2, userId);
        ps.setString(3, type);
        ps.setString(4, major);
        ps.setString(5, institution);
        ps.executeUpdate();
    }

    System.out.println("\nAdd Transcript for degree_id = " + degreeId);

    System.out.print("transcript_id (NUMBER): ");
    int transcriptId = Integer.parseInt(in.nextLine().trim());

    System.out.print("Term name (e.g., Fall 2024): ");
    String tName = in.nextLine().trim();

    System.out.print("GPA (0.00 - 4.00): ");
    double gpa = Double.parseDouble(in.nextLine().trim());

    String sqlTrans = "INSERT INTO transcript "
            + "(transcript_id, degree_id, t_name, gpa) "
            + "VALUES (?, ?, ?, ?)";

    try (PreparedStatement ps = conn.prepareStatement(sqlTrans)) {
        ps.setInt(1, transcriptId);
        ps.setInt(2, degreeId);
        ps.setString(3, tName);
        ps.setDouble(4, gpa);
        ps.executeUpdate();
    }

//Transcript Line Adder
    System.out.println("\nAdd at least one Transcript Line for transcript_id = " + transcriptId);

    System.out.print("Course name: ");
    String course = in.nextLine().trim();

    System.out.print("Grade (e.g., A, B+): ");
    String grade = in.nextLine().trim();

    System.out.print("Class average (blank = NULL): ");
    String classAvgStr = in.nextLine().trim();

    String sqlLine = "INSERT INTO transcript_line "
            + "(transcript_id, course, grade, class_avg) "
            + "VALUES (?, ?, ?, ?)";

    try (PreparedStatement ps = conn.prepareStatement(sqlLine)) {
        ps.setInt(1, transcriptId);
        ps.setString(2, course);
        ps.setString(3, grade);

        if (classAvgStr.isEmpty()) {
            ps.setNull(4, java.sql.Types.NUMERIC);
        } else {
            ps.setDouble(4, Double.parseDouble(classAvgStr));
        }

        ps.executeUpdate();
    }
}

private static void addExperienceForUser(Connection conn, Scanner in, String userId) throws SQLException {
    System.out.println("\nAdd Experience for " + userId);

    System.out.print("experience_id (NUMBER): ");
    int expId = Integer.parseInt(in.nextLine().trim());

    System.out.print("Company name: ");
    String companyName = in.nextLine().trim();

    System.out.print("Role: ");
    String role = in.nextLine().trim();

    System.out.print("Start date (YYYY-MM-DD): ");
    String startStr = in.nextLine().trim();
    java.sql.Date startDate = java.sql.Date.valueOf(startStr);

    System.out.print("End date (YYYY-MM-DD, blank = NULL): ");
    String endStr = in.nextLine().trim();
    java.sql.Date endDate = null;
    if (!endStr.isEmpty()) {
        endDate = java.sql.Date.valueOf(endStr);
    }

    // 1) Insert into supertype: experience
    String sqlExp = "INSERT INTO experience "
            + "(experience_id, user_id, company_name, role, start_date, end_date) "
            + "VALUES (?, ?, ?, ?, ?, ?)";

    try (PreparedStatement ps = conn.prepareStatement(sqlExp)) {
        ps.setInt(1, expId);
        ps.setString(2, userId);
        ps.setString(3, companyName);
        ps.setString(4, role);
        ps.setDate(5, startDate);

        if (endDate == null) {
            ps.setNull(6, java.sql.Types.DATE);
        } else {
            ps.setDate(6, endDate);
        }

        ps.executeUpdate();
    }

    // 2)Specifies work or internship experience
    System.out.print("Is this (W)ork or (I)nternship experience? [W/I]: ");
    String kind = in.nextLine().trim().toUpperCase();

    if (kind.startsWith("W")) {
        // Work experience subclass
        String sqlWork = "INSERT INTO work_experience (experience_id) VALUES (?)";
        try (PreparedStatement ps = conn.prepareStatement(sqlWork)) {
            ps.setInt(1, expId);
            ps.executeUpdate();
        }
    } else if (kind.startsWith("I")) {
        // Internship subclass
        System.out.print("Mentor name: ");
        String mentorName = in.nextLine().trim();

        String sqlIntern = "INSERT INTO internship_experience (experience_id, mentor_name) "
                         + "VALUES (?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sqlIntern)) {
            ps.setInt(1, expId);
            ps.setString(2, mentorName);
            ps.executeUpdate();
        }
    } else {
        System.out.println("No subtype selected (neither Work nor Internship). "
                + "This experience will be shown as a work exp/intern exp.\n");
    }
}

private static void deleteApplicant(Connection conn, Scanner in) throws SQLException {
    System.out.println("\nDelete Applicant");
    System.out.print("Enter user_id to delete: ");
    String userId = in.nextLine().trim();

    // 1) Delete transcript-related rows
    runDeleteWithUser(conn,
        "DELETE FROM transcript_line tl " +
        "WHERE tl.transcript_id IN (" +
        "  SELECT t.transcript_id " +
        "  FROM transcript t " +
        "  JOIN educational_degree d ON d.degree_id = t.degree_id " +
        "  WHERE d.user_id = ?" +
        ")", userId);

    runDeleteWithUser(conn,
        "DELETE FROM transcript t " +
        "WHERE t.degree_id IN (" +
        "  SELECT d.degree_id FROM educational_degree d WHERE d.user_id = ?" +
        ")", userId);

    runDeleteWithUser(conn,
        "DELETE FROM educational_degree WHERE user_id = ?", userId);

    // 2) Delete experience subtypes then experience
    runDeleteWithUser(conn,
        "DELETE FROM work_experience we " +
        "WHERE we.experience_id IN (" +
        "  SELECT e.experience_id FROM experience e WHERE e.user_id = ?" +
        ")", userId);

    runDeleteWithUser(conn,
        "DELETE FROM internship_experience ie " +
        "WHERE ie.experience_id IN (" +
        "  SELECT e.experience_id FROM experience e WHERE e.user_id = ?" +
        ")", userId);

    runDeleteWithUser(conn,
        "DELETE FROM experience WHERE user_id = ?", userId);

    // 3) Delete applications and reviews
    runDeleteWithUser(conn,
        "DELETE FROM application WHERE user_id = ?", userId);

    runDeleteWithUser(conn,
        "DELETE FROM company_review WHERE user_id = ?", userId);

    // 4)  delete applicant
    int rows = runDeleteWithUser(conn,
        "DELETE FROM applicant WHERE user_id = ?", userId);

    if (rows == 0) {
        System.out.println("No applicant found with user_id = " + userId + ".\n");
        conn.rollback();
    } else {
        conn.commit();
        System.out.println("Deleted applicant " + userId + " and related data.\n");
    }
}

private static int runDeleteWithUser(Connection conn, String sql, String userId) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, userId);
        return ps.executeUpdate();
    }
}


private static void updateApplicantEmail(Connection conn, Scanner in) throws SQLException {
    System.out.println("\nUpdate Applicant Email");
    System.out.print("Enter user_id: ");
    String userId = in.nextLine().trim();

    System.out.print("Enter new email: ");
    String newEmail = in.nextLine().trim();

    String sql = "UPDATE applicant SET email = ? WHERE user_id = ?";

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, newEmail);
        ps.setString(2, userId);

        int rows = ps.executeUpdate();
        if (rows == 0) {
            System.out.println("No applicant found with user_id = " + userId + ".\n");
        } else {
            conn.commit();
            System.out.println("Updated email for " + rows + " applicant(s).\n");
        }
    }
}

private static void searchApplicant(Connection conn, Scanner in) throws SQLException {
    System.out.println("\nSearch Applicant");
    System.out.print("Enter user_id to search: ");
    String userId = in.nextLine().trim();

    String sql = "SELECT * FROM applicant WHERE user_id = ?";

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, userId);
        try (ResultSet rs = ps.executeQuery()) {
            printResultSet(rs, 50);
        }
    }
}

//JOB POSTING MANAGER MENU AND OPTION METHODS

private static void manageJobPostings(Connection conn, Scanner in) throws SQLException {
    while (true) {
        System.out.println("\nWelcome to the Job Posting Manager!");
        System.out.println(" 1) Add Job Posting");
        System.out.println(" 2. Delete Job Posting");
        System.out.println(" 3. Update Job Salary Range");
        System.out.println(" 4. Search Job Posting by job_id");
        System.out.println(" B. Back to Main Menu");
        System.out.print("Choose: ");
        String choice = in.nextLine().trim().toUpperCase();
            switch (choice) {
                case "1":
                    addJobPosting(conn, in);
                    break;
                case "2":
                    deleteJobPosting(conn, in);
                    break;
                case "3":
                    updateJobPostingSalary(conn, in);
                    break;
                case "4":
                    searchJobPosting(conn, in);
                    break;
                case "B":
                    return;
                default:
                    System.out.println("Invalid Inpput.\n");
            }
    }
}

private static void addJobPosting(Connection conn, Scanner in) throws SQLException {
    System.out.println("\nAdd Job Posting");

    System.out.print("job_id: ");
    String jobId = in.nextLine().trim();

    System.out.print("admin_id (hiring_manager.admin_id): ");
    String adminIdStr = in.nextLine().trim();
    int adminId = Integer.parseInt(adminIdStr);

    System.out.print("Title: ");
    String title = in.nextLine().trim();

    System.out.print("Description: ");
    String description = in.nextLine().trim();

    System.out.print("Requirements: ");
    String requirements = in.nextLine().trim();

    System.out.print("Minimum Salary (blank = NULL): ");
    String minStr = in.nextLine().trim();

    System.out.print("Maximum Salary (blank = NULL): ");
    String maxStr = in.nextLine().trim();

    System.out.print("Posted Date (YYYY-MM-DD): ");
    String dateStr = in.nextLine().trim();
    java.sql.Date postedDate = java.sql.Date.valueOf(dateStr);

    String sql = "INSERT INTO job_posting "
               + "(job_id, admin_id, title, description, requirements, salary_min, salary_max, posted_date) "
               + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, jobId);
        ps.setInt(2, adminId);
        ps.setString(3, title);
        ps.setString(4, description);
        ps.setString(5, requirements);

        if (minStr.isEmpty()) {
            ps.setNull(6, java.sql.Types.NUMERIC);
        } else {
            ps.setDouble(6, Double.parseDouble(minStr));
        }

        if (maxStr.isEmpty()) {
            ps.setNull(7, java.sql.Types.NUMERIC);
        } else {
            ps.setDouble(7, Double.parseDouble(maxStr));
        }

        ps.setDate(8, postedDate);

        int rows = ps.executeUpdate();
        conn.commit();
        System.out.println("Inserted " + rows + " job posting(s).\n");
    }
}

private static void deleteJobPosting(Connection conn, Scanner in) throws SQLException {
    System.out.println("\n--- Delete Job Posting ---");
    System.out.print("Enter job_id to delete: ");
    String jobId = in.nextLine().trim();

    String sql = "DELETE FROM job_posting WHERE job_id = ?";

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, jobId);
        int rows = ps.executeUpdate();
        if (rows == 0) {
            System.out.println("No job posting found with job_id = " + jobId + ".\n");
        } else {
            conn.commit();
            System.out.println("Deleted " + rows + " job posting(s).\n");
        }
    }
}

private static void updateJobPostingSalary(Connection conn, Scanner in) throws SQLException {
    System.out.println("\nUpdate Job Salary Range");
    System.out.print("Enter job_id: ");
    String jobId = in.nextLine().trim();

    System.out.print("New Minimum Salary (blank = NULL): ");
    String minStr = in.nextLine().trim();

    System.out.print("New Maximum Salary (blank = NULL): ");
    String maxStr = in.nextLine().trim();

    String sql = "UPDATE job_posting SET salary_min = ?, salary_max = ? WHERE job_id = ?";

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        if (minStr.isEmpty()) {
            ps.setNull(1, java.sql.Types.NUMERIC);
        } else {
            ps.setDouble(1, Double.parseDouble(minStr));
        }

        if (maxStr.isEmpty()) {
            ps.setNull(2, java.sql.Types.NUMERIC);
        } else {
            ps.setDouble(2, Double.parseDouble(maxStr));
        }

        ps.setString(3, jobId);

        int rows = ps.executeUpdate();
        if (rows == 0) {
            System.out.println("No job posting found with job_id = " + jobId + ".\n");
        } else {
            conn.commit();
            System.out.println("Updated salary range for " + rows + " job posting(s).\n");
        }
    }
}

private static void searchJobPosting(Connection conn, Scanner in) throws SQLException {
    System.out.println("\n--- Search Job Posting ---");
    System.out.print("Enter job_id to search: ");
    String jobId = in.nextLine().trim();

    String sql = "SELECT * FROM job_posting WHERE job_id = ?";

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, jobId);
        try (ResultSet rs = ps.executeQuery()) {
            printResultSet(rs, 50);
        }
    }
}

        //SQL splitter: splits on ';' outside single quotes.
       //IGnores blank lines and SQL*Plus directives.
       //Adapted from: https://www.geeksforgeeks.org/java/how-to-execute-multiple-sql-commands-on-a-database-simultaneously-in-jdbc/,
    //                 https://www.baeldung.com/java-jdbc-execute-multiple-statements 
       private static void runSqlBatch(Connection conn, String sql, boolean ignoreErrors, boolean showResults) throws SQLException {
        List<String> stmts = splitSqlStatements(sql);
        try (Statement st = conn.createStatement()) {
            for (String s : stmts) {
                String t = s.trim();
                if (t.isEmpty()) continue;

                String up = t.toUpperCase(Locale.ROOT);
                if (up.startsWith("SET ") || up.startsWith("PROMPT ")
                        || up.equals("EXIT") || up.startsWith("WHENEVER SQLERROR")) {
                    continue;
                }

                try {
                    boolean hasResult = st.execute(t);
                    if (hasResult && showResults) {
                        try (ResultSet rs = st.getResultSet()) {
                            printResultSet(rs, 200);
                        }
                    }
                } catch (SQLException ex) {
                    if (ignoreErrors) {
                        System.out.println("(ignored) " + ex.getMessage());
                    } else {
                        throw ex;
                    }
                }
            }
            conn.commit();
        }
    }

    private static List<String> splitSqlStatements(String sql) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inSingle = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') inSingle = !inSingle;
            if (c == ';' && !inSingle) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        if (cur.toString().trim().length() > 0) out.add(cur.toString());
        return out;
    }

    private static void printResultSet(ResultSet rs, int maxRows) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();

        for (int c = 1; c <= cols; c++) {
            if (c > 1) System.out.print(" | ");
            System.out.print(md.getColumnLabel(c));
        }
        System.out.println();
        System.out.println("------------------------------------------------------------");

        int row = 0;
        while (rs.next() && row < maxRows) {
            for (int c = 1; c <= cols; c++) {
                if (c > 1) System.out.print(" | ");
                String val = rs.getString(c);
                System.out.print(val == null ? "NULL" : val);
            }
            System.out.println();
            row++;
        }
        if (row == 0) System.out.println("(no rows)");
        System.out.println();
    }
}
