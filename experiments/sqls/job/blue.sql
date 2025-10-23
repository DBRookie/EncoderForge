SET
    max_parallel_workers_per_gather = 3;

SET
    statement_timeout = 3600000;

RESET enable_mergejoin;

RESET enable_nestloop;

EXPLAIN ANALYZE
SELECT
    CASE WHEN 1 / (1 + EXP(-("Salary_USD"[1]::float * -0.712591::float+"Salary_USD"[2]::float * 0.452292::float+"Salary_USD"[3]::float * -1.058845::float+"Salary_USD"[4]::float * 0.153614::float+"Salary_USD"[5]::float * -0.840761::float+"Salary_USD"[6]::float * 0.276129::float+"Salary_USD"[7]::float * 0.636057::float+"Salary_USD"[8]::float * 0.211309::float+"Salary_USD"[9]::float * 0.349776::float+"Salary_USD"[10]::float * -0.075537::float+"Salary_USD"[11]::float * 0.542779::float+"Salary_USD"[12]::float * 0.087973::float+"Salary_USD"[13]::float * 0.040441::float+"Salary_USD"[14]::float * -0.226301::float+"Salary_USD"[15]::float * -1.077645::float+"Salary_USD"[16]::float * -0.516830::float+"Salary_USD"[17]::float * 0.551906::float+"Salary_USD"[18]::float * 0.556395::float+"Salary_USD"[19]::float * 0.304668::float+"Salary_USD"[20]::float * 0.345099::float+"Job_Title"[1]::float * -0.525462::float+"Job_Title"[2]::float * 0.203001::float+"Job_Title"[3]::float * 0.304853::float+"Job_Title"[4]::float * 0.094340::float+"Job_Title"[5]::float * 0.213967::float+"Job_Title"[6]::float * -0.094012::float+"Job_Title"[7]::float * 0.224246::float+"Job_Title"[8]::float * 0.309708::float+"Job_Title"[9]::float * -0.216898::float+"Job_Title"[10]::float * -0.513817::float+"Industry"[1]::float * 0.165102::float+"Industry"[2]::float * 0.204235::float+"Industry"[3]::float * 0.380563::float+"Industry"[4]::float * 0.134490::float+"Industry"[5]::float * 0.016476::float+"Industry"[6]::float * -0.413001::float+"Industry"[7]::float * -0.198342::float+"Industry"[8]::float * -0.189079::float+"Industry"[9]::float * 0.096681::float+"Industry"[10]::float * -0.197197::float+"Company_Size"[1]::float * -0.203286::float+"Company_Size"[2]::float * 0.123797::float+"Company_Size"[3]::float * 0.079417::float+"Location"[1]::float * -0.277282::float+"Location"[2]::float * 0.365472::float+"Location"[3]::float * 0.093396::float+"Location"[4]::float * 0.027884::float+"Location"[5]::float * 0.236091::float+"Location"[6]::float * 0.182208::float+"Location"[7]::float * -0.236322::float+"Location"[8]::float * -0.610846::float+"Location"[9]::float * 0.067818::float+"Location"[10]::float * 0.151509::float+"AI_Adoption_Level"[1]::float * -0.116731::float+"AI_Adoption_Level"[2]::float * 0.230762::float+"AI_Adoption_Level"[3]::float * -0.114103::float+"Automation_Risk"[1]::float * -0.032276::float+"Automation_Risk"[2]::float * 0.175759::float+"Automation_Risk"[3]::float * -0.143555::float+"Required_Skills"[1]::float * 0.212450::float+"Required_Skills"[2]::float * -0.099472::float+"Required_Skills"[3]::float * 0.387550::float+"Required_Skills"[4]::float * 0.301891::float+"Required_Skills"[5]::float * -0.061487::float+"Required_Skills"[6]::float * -0.338081::float+"Required_Skills"[7]::float * -0.184563::float+"Required_Skills"[8]::float * -0.161928::float+"Required_Skills"[9]::float * 0.001843::float+"Required_Skills"[10]::float * -0.058275::float+"Job_Growth_Projection"[1]::float * -0.067666::float+"Job_Growth_Projection"[2]::float * -0.019018::float+"Job_Growth_Projection"[3]::float * 0.086612::float+ -0.14418868017081352))) <= 0.5 THEN 'No' ELSE 'Yes' END 
FROM
    (
        SELECT
            "Job_Title",
            "Industry",
            "Company_Size",
            "Location",
            "AI_Adoption_Level",
            "Automation_Risk",
            "Required_Skills",
            "salary_usd_array_list" AS "Salary_USD",
            "Job_Growth_Projection"
        FROM
            (
                SELECT
                    "Job_Title",
                    "Industry",
                    "Company_Size",
                    "Location",
                    "AI_Adoption_Level",
                    "Automation_Risk",
                    "Required_Skills",
                    CASE
                        WHEN "Salary_USD" >= 35963.29731701118
                        AND "Salary_USD" < 41925.62353185205 THEN 0
                        WHEN "Salary_USD" >= 41925.62353185205
                        AND "Salary_USD" < 47887.94974669291 THEN 1
                        WHEN "Salary_USD" >= 47887.94974669291
                        AND "Salary_USD" < 53850.27596153377 THEN 2
                        WHEN "Salary_USD" >= 53850.27596153377
                        AND "Salary_USD" < 59812.60217637464 THEN 3
                        WHEN "Salary_USD" >= 59812.60217637464
                        AND "Salary_USD" < 65774.92839121551 THEN 4
                        WHEN "Salary_USD" >= 65774.92839121551
                        AND "Salary_USD" < 71737.25460605638 THEN 5
                        WHEN "Salary_USD" >= 71737.25460605638
                        AND "Salary_USD" < 77699.58082089723 THEN 6
                        WHEN "Salary_USD" >= 77699.58082089723
                        AND "Salary_USD" < 83661.9070357381 THEN 7
                        WHEN "Salary_USD" >= 83661.9070357381
                        AND "Salary_USD" < 89624.23325057897 THEN 8
                        WHEN "Salary_USD" >= 89624.23325057897
                        AND "Salary_USD" < 95586.55946541982 THEN 9
                        WHEN "Salary_USD" >= 95586.55946541982
                        AND "Salary_USD" < 101548.88568026069 THEN 10
                        WHEN "Salary_USD" >= 101548.88568026069
                        AND "Salary_USD" < 107511.21189510156 THEN 11
                        WHEN "Salary_USD" >= 107511.21189510156
                        AND "Salary_USD" < 113473.53810994243 THEN 12
                        WHEN "Salary_USD" >= 113473.53810994243
                        AND "Salary_USD" < 119435.86432478328 THEN 13
                        WHEN "Salary_USD" >= 119435.86432478328
                        AND "Salary_USD" < 125398.19053962415 THEN 14
                        WHEN "Salary_USD" >= 125398.19053962415
                        AND "Salary_USD" < 131360.516754465 THEN 15
                        WHEN "Salary_USD" >= 131360.516754465
                        AND "Salary_USD" < 137322.84296930587 THEN 16
                        WHEN "Salary_USD" >= 137322.84296930587
                        AND "Salary_USD" < 143285.16918414674 THEN 17
                        WHEN "Salary_USD" >= 143285.16918414674
                        AND "Salary_USD" < 149247.4953989876 THEN 18
                        ELSE 19
                    END AS "Salary_USD",
                    "Job_Growth_Projection"
                FROM
                    (
                        SELECT
                            "job_title_array_list" AS "Job_Title",
                            "industry_array_list" AS "Industry",
                            "company_size_array_list" AS "Company_Size",
                            "location_array_list" AS "Location",
                            "ai_adoption_level_array_list" AS "AI_Adoption_Level",
                            "automation_risk_array_list" AS "Automation_Risk",
                            "required_skills_array_list" AS "Required_Skills",
                            "Salary_USD",
                            "job_growth_projection_array_list" AS "Job_Growth_Projection"
                        FROM
                            job
                            left join job_title_array on job."Job_Title" = job_title_array."job_title"
                            left join industry_array on job."Industry" = industry_array."industry"
                            left join company_size_array on job."Company_Size" = company_size_array."company_size"
                            left join location_array on job."Location" = location_array."location"
                            left join ai_adoption_level_array on job."AI_Adoption_Level" = ai_adoption_level_array."ai_adoption_level"
                            left join automation_risk_array on job."Automation_Risk" = automation_risk_array."automation_risk"
                            left join required_skills_array on job."Required_Skills" = required_skills_array."required_skills"
                            left join job_growth_projection_array on job."Job_Growth_Projection" = job_growth_projection_array."job_growth_projection"
                    ) AS data
            ) AS data
            left join salary_usd_array on data."Salary_USD" = salary_usd_array."salary_usd"
    ) AS data